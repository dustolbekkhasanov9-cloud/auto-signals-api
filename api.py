from news_feed import build_feed
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query

from signal_engine import analyze_symbol


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("autosignal-api")

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    logger.warning("DATABASE_URL not set")


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

REFRESH_SECONDS = 10
WAITING_RETRY_SECONDS = 15
ACTIVE_EXPIRE_GRACE_SECONDS = 65
MAX_HISTORY_ITEMS = 2000
POSTGRES_STATEMENT_TIMEOUT_MS = 5000

SCAN_TIMEFRAMES = ["5m", "10m", "30m", "1h"]
SCAN_DURATION_TYPES = ["short", "long"]

ANALYZE_CONCURRENCY = 4

MIN_CONFIDENCE_TO_KEEP = 30.0
MIN_VOLATILITY_RATIO = 0.75
REQUIRE_AT_LEAST_ONE_BIAS_MATCH = True

signal_cache: Dict[str, Dict[str, Any]] = {}
scan_cache: Dict[str, Dict[str, Any]] = {}
last_updated_at: str | None = None
last_refresh_status: str = "starting"

active_signals: list[dict] = []
signal_history: list[dict] = []

refresh_lock = asyncio.Lock()


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


def get_pg_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        connect_timeout=5,
        options=f"-c statement_timeout={POSTGRES_STATEMENT_TIMEOUT_MS}",
    )


def init_postgres() -> None:
    conn = get_pg_connection()
    cur = conn.cursor()
    try:
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

    return (
        item.get("symbol"),
        item.get("signal"),
        item.get("timeframe"),
        item.get("duration_type"),
        entry_time_iso,
        exit_time_iso,
    )


def make_signal_key_str(item: dict) -> str:
    key = make_signal_key(item)
    return "|".join("" if x is None else str(x) for x in key)


def upsert_active_signals(cur, items: list[dict]) -> None:
    current_keys = set()

    for item in items:
        signal_key = make_signal_key_str(item)
        current_keys.add(signal_key)

        cur.execute(
            """
            INSERT INTO active_signals_pg (signal_key, payload, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (signal_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = NOW()
            """,
            (signal_key, json.dumps(item, ensure_ascii=False)),
        )

    cur.execute("SELECT signal_key FROM active_signals_pg;")
    existing_rows = cur.fetchall()
    existing_keys = {row["signal_key"] for row in existing_rows} if existing_rows else set()

    keys_to_delete = list(existing_keys - current_keys)

    if keys_to_delete:
        cur.execute(
            "DELETE FROM active_signals_pg WHERE signal_key = ANY(%s);",
            (keys_to_delete,),
        )


def upsert_history_signals(cur, items: list[dict]) -> None:
    for item in items:
        signal_key = make_signal_key_str(item)
        cur.execute(
            """
            INSERT INTO signal_history_pg (signal_key, payload, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (signal_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = NOW()
            """,
            (signal_key, json.dumps(item, ensure_ascii=False)),
        )


def trim_history_table(cur) -> None:
    cur.execute("SELECT COUNT(*) AS count FROM signal_history_pg;")
    row = cur.fetchone()
    total = int(row["count"]) if row and row["count"] is not None else 0

    if total <= MAX_HISTORY_ITEMS:
        return

    to_delete = total - MAX_HISTORY_ITEMS

    cur.execute(
        """
        DELETE FROM signal_history_pg
        WHERE id IN (
            SELECT id
            FROM signal_history_pg
            ORDER BY id ASC
            LIMIT %s
        );
        """,
        (to_delete,),
    )


def save_state() -> None:
    global active_signals, signal_history

    conn = None
    cur = None

    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        upsert_active_signals(cur, active_signals)
        upsert_history_signals(cur, signal_history)
        trim_history_table(cur)

        conn.commit()

    except Exception as e:
        logger.exception("SAVE STATE FAILED: %s", e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def load_state_from_postgres() -> None:
    global active_signals, signal_history

    conn = get_pg_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT payload FROM active_signals_pg ORDER BY id ASC;")
        active_rows = cur.fetchall()

        cur.execute(
            "SELECT payload FROM signal_history_pg ORDER BY id DESC LIMIT %s;",
            (MAX_HISTORY_ITEMS,),
        )
        history_rows = cur.fetchall()

        active_signals = [row["payload"] for row in active_rows] if active_rows else []
        signal_history = [row["payload"] for row in history_rows] if history_rows else []

        if not isinstance(active_signals, list):
            active_signals = []

        if not isinstance(signal_history, list):
            signal_history = []

    finally:
        cur.close()
        conn.close()


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


def deduplicate_signal_history() -> None:
    global signal_history

    seen = set()
    unique_items = []

    for item in signal_history:
        key = make_signal_key(item)
        if key in seen:
            continue
        seen.add(key)
        unique_items.append(item)

    signal_history = unique_items[:MAX_HISTORY_ITEMS]


def history_duplicate_exists(item: dict) -> bool:
    try:
        item_key = make_signal_key(item)
        for h in signal_history:
            try:
                if make_signal_key(h) == item_key:
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False


def safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def parse_chart_label_to_utc(label: Any) -> datetime | None:
    if not label:
        return None

    text = str(label).strip()

    try:
        return parse_iso_utc(text)
    except Exception:
        pass

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass

    return None


def get_exit_price_from_chart(item: dict) -> float | None:
    exit_time_iso = item.get("exit_time_iso")
    if not exit_time_iso:
        return None

    try:
        exit_dt = parse_iso_utc(exit_time_iso)
    except Exception:
        return None

    symbol = item.get("symbol")
    timeframe = item.get("timeframe")
    duration_type = item.get("duration_type")

    candidates = []

    if symbol and timeframe and duration_type:
        cache_key = make_cache_key(symbol, timeframe, duration_type)
        cached = scan_cache.get(cache_key)
        if isinstance(cached, dict):
            candidates.append(cached)

    candidates.append(item)

    best_future_price = None
    best_future_dt = None

    best_past_price = None
    best_past_dt = None

    for source in candidates:
        labels = source.get("chart_labels")
        prices = source.get("chart_prices")

        if not isinstance(labels, list) or not isinstance(prices, list):
            continue

        if len(labels) == 0 or len(prices) == 0:
            continue

        for label, raw_price in zip(labels, prices):
            candle_dt = parse_chart_label_to_utc(label)
            price = safe_float(raw_price)

            if candle_dt is None or price is None:
                continue

            if candle_dt >= exit_dt:
                if best_future_dt is None or candle_dt < best_future_dt:
                    best_future_dt = candle_dt
                    best_future_price = price
            else:
                if best_past_dt is None or candle_dt > best_past_dt:
                    best_past_dt = candle_dt
                    best_past_price = price

    if best_future_price is not None:
        return round(best_future_price, 5)

    if best_past_price is not None:
        return round(best_past_price, 5)

    return None


def get_latest_exit_price_for_item(item: dict) -> float | None:
    chart_exit_price = get_exit_price_from_chart(item)
    if chart_exit_price is not None:
        return chart_exit_price

    symbol = item.get("symbol")
    timeframe = item.get("timeframe")
    duration_type = item.get("duration_type")

    if symbol and timeframe and duration_type:
        cache_key = make_cache_key(symbol, timeframe, duration_type)
        cached = scan_cache.get(cache_key)

        if isinstance(cached, dict):
            price = safe_float(cached.get("price"))
            if price is not None:
                return round(price, 5)

    price = safe_float(item.get("price"))
    if price is not None:
        return round(price, 5)

    chart = item.get("chart_prices")
    if isinstance(chart, list) and len(chart) > 0:
        last_chart_price = safe_float(chart[-1])
        if last_chart_price is not None:
            return round(last_chart_price, 5)

    return None


def finalize_closed_signal(
    item: dict,
    exit_price: float | None,
    close_reason: str = "",
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
        item["last_fact_retry_iso"] = now_iso()
        return item

    try:
        entry_price = float(entry_price)
    except Exception:
        item["result"] = "WAITING_RESULT"
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
        return item

    if exit_price is None:
        exit_price = get_latest_exit_price_for_item(item)

    if exit_price is None:
        item["result"] = "WAITING_RESULT"
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
        return item

    try:
        exit_price = float(exit_price)
    except Exception:
        item["result"] = "WAITING_RESULT"
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
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


def apply_multitimeframe_confirmation(item: dict) -> dict:
    confirm_bias = item.get("confirm_bias")
    trend_bias = item.get("trend_bias")
    signal = item.get("signal")
    confidence = item.get("confidence", 50)

    if signal == confirm_bias == trend_bias:
        confidence *= 1.25
    elif signal == confirm_bias:
        confidence *= 1.1
    else:
        confidence *= 0.85

    item["confidence"] = round(confidence, 1)
    return item


def apply_volatility_filter(item: dict) -> dict:
    volatility = item.get("volatility_ratio")
    if volatility is None:
        return item

    confidence = item.get("confidence", 50)

    if volatility < 0.7:
        confidence *= 0.7
    elif 0.8 <= volatility <= 1.2:
        confidence *= 1.15
    elif volatility > 1.5:
        confidence *= 0.85

    item["confidence"] = round(confidence, 1)
    return item


def apply_market_regime_bonus(item: dict) -> dict:
    regime = item.get("market_regime")
    strategy = item.get("strategy")
    confidence = item.get("confidence", 50)

    strategy_text = str(strategy).lower()

    if regime == "TREND" and "trend" in strategy_text:
        confidence *= 1.2
    elif regime == "RANGE" and "rsi" in strategy_text:
        confidence *= 1.2
    elif regime == "VOLATILE" and "breakout" in strategy_text:
        confidence *= 1.2
    else:
        confidence *= 0.9

    item["confidence"] = round(confidence, 1)
    return item


def calculate_strategy_stats() -> dict:
    stats = {}

    for item in signal_history:
        strategy = item.get("strategy")
        if not strategy:
            continue

        result = item.get("result")
        if result not in ("TP", "SL"):
            continue

        if strategy not in stats:
            stats[strategy] = {"tp": 0, "sl": 0}

        if result == "TP":
            stats[strategy]["tp"] += 1
        else:
            stats[strategy]["sl"] += 1

    winrate = {}

    for strategy, data in stats.items():
        total = data["tp"] + data["sl"]
        if total == 0:
            continue
        winrate[strategy] = data["tp"] / total

    return winrate


def add_signals_to_active(items: list[dict]) -> None:
    global active_signals

    stats = calculate_strategy_stats()
    existing_keys = {make_signal_key(s) for s in active_signals}

    for item in items:
        item = apply_market_regime_bonus(item)
        item = apply_multitimeframe_confirmation(item)
        item = apply_volatility_filter(item)

        strategy = item.get("strategy")
        if strategy in stats:
            winrate = stats[strategy]
            item["confidence"] = round(item.get("confidence", 50) * (0.5 + winrate), 1)

        try:
            confidence_value = float(item.get("confidence", 50) or 50)
        except Exception:
            confidence_value = 50.0

        item["confidence"] = round(max(1.0, min(95.0, confidence_value)), 1)

        signal = item.get("signal")
        if signal not in ("BUY", "SELL"):
            continue

        volatility = item.get("volatility_ratio")
        if volatility is not None:
            try:
                if float(volatility) < MIN_VOLATILITY_RATIO:
                    logger.info(
                        "SIGNAL FILTERED BY VOLATILITY: %s %s vol=%s",
                        item.get("symbol"),
                        item.get("timeframe"),
                        volatility,
                    )
                    continue
            except Exception:
                pass

        if REQUIRE_AT_LEAST_ONE_BIAS_MATCH:
            matches = 0
            if item.get("confirm_bias") == signal:
                matches += 1
            if item.get("trend_bias") == signal:
                matches += 1

            if matches == 0:
                logger.info(
                    "SIGNAL FILTERED BY BIAS MISMATCH: %s %s signal=%s confirm=%s trend=%s",
                    item.get("symbol"),
                    item.get("timeframe"),
                    signal,
                    item.get("confirm_bias"),
                    item.get("trend_bias"),
                )
                continue

        if item["confidence"] < MIN_CONFIDENCE_TO_KEEP:
            logger.info(
                "SIGNAL FILTERED BY MIN_CONFIDENCE: %s %s conf=%s",
                item.get("symbol"),
                item.get("timeframe"),
                item["confidence"],
            )
            continue

        entry_time_iso = item.get("entry_time_iso")
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


def update_closed_history_results() -> bool:
    global active_signals, signal_history

    now_utc = datetime.now(timezone.utc)
    still_active: list[dict] = []
    changed = False

    old_active_count = len(active_signals)
    old_history_count = len(signal_history)

    for item in active_signals:
        if item.get("result") != "OPEN":
            still_active.append(item)
            continue

        exit_time_iso = item.get("exit_time_iso", "")

        if not exit_time_iso:
            still_active.append(item)
            continue

        try:
            exit_dt = parse_iso_utc(exit_time_iso)
        except Exception:
            logger.warning("ACTIVE SIGNAL BAD EXIT TIME, KEEPING ACTIVE: %s", item)
            still_active.append(item)
            continue

        seconds_after_expiry = (now_utc - exit_dt).total_seconds()
        if seconds_after_expiry < ACTIVE_EXPIRE_GRACE_SECONDS:
            still_active.append(item)
            continue

        exit_price = get_latest_exit_price_for_item(item)
        close_reason = "exit_price_from_chart_or_cache"

        closed_item = finalize_closed_signal(
            item,
            exit_price=exit_price,
            close_reason=close_reason if exit_price is not None else "exit_price_not_found",
        )

        if not history_duplicate_exists(closed_item):
            signal_history.insert(0, closed_item)
            changed = True

        if closed_item.get("result") == "WAITING_RESULT":
            logger.info(
                "CLOSE WAITING_RESULT: %s %s",
                item.get("symbol"),
                exit_time_iso,
            )
        else:
            logger.info(
                "CLOSE FINALIZED: %s %s exit_price=%s result=%s",
                item.get("symbol"),
                exit_time_iso,
                closed_item.get("exit_price"),
                closed_item.get("result"),
            )

    if len(still_active) != old_active_count:
        changed = True

    active_signals = still_active
    signal_history = signal_history[:MAX_HISTORY_ITEMS]
    deduplicate_signal_history()

    logger.info(
        "HISTORY UPDATE: active=%s history=%s waiting=%s tp=%s sl=%s",
        len(active_signals),
        len(signal_history),
        len([x for x in signal_history if x.get("result") == "WAITING_RESULT"]),
        len([x for x in signal_history if x.get("result") == "TP"]),
        len([x for x in signal_history if x.get("result") == "SL"]),
    )

    if changed or len(signal_history) != old_history_count:
        save_state()

    return changed


def update_waiting_history_results() -> bool:
    global signal_history

    now_utc = datetime.now(timezone.utc)
    updated = False

    for idx, item in enumerate(signal_history):
        if item.get("result") != "WAITING_RESULT":
            continue

        last_retry_iso = item.get("last_fact_retry_iso")
        if last_retry_iso:
            try:
                last_retry_dt = parse_iso_utc(last_retry_iso)
                if (now_utc - last_retry_dt).total_seconds() < WAITING_RETRY_SECONDS:
                    continue
            except Exception:
                pass

        exit_price = get_latest_exit_price_for_item(item)

        if exit_price is None:
            signal_history[idx]["last_fact_retry_iso"] = now_iso()
            updated = True
            continue

        signal_history[idx] = finalize_closed_signal(
            item,
            exit_price=exit_price,
            close_reason="waiting_result_resolved_by_chart_or_cache",
        )
        updated = True

    if updated:
        signal_history = signal_history[:MAX_HISTORY_ITEMS]
        deduplicate_signal_history()

        logger.info(
            "WAITING RESOLVE: history=%s waiting=%s tp=%s sl=%s",
            len(signal_history),
            len([x for x in signal_history if x.get("result") == "WAITING_RESULT"]),
            len([x for x in signal_history if x.get("result") == "TP"]),
            len([x for x in signal_history if x.get("result") == "SL"]),
        )

        save_state()

    return updated


def safe_reconcile_for_api(update_waiting: bool = False) -> None:
    try:
        update_closed_history_results()
        if update_waiting:
            update_waiting_history_results()
    except Exception as e:
        logger.exception("API RECONCILE FAILED: %s", e)


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


async def run_bounded_analysis(symbol: str, timeframe: str, duration_type: str, sem: asyncio.Semaphore) -> dict:
    async with sem:
        return await analyze_symbol_safe(symbol, timeframe, duration_type)


async def refresh_all_signals() -> None:
    global signal_cache, scan_cache, last_updated_at, last_refresh_status

    async with refresh_lock:
        started_at = time.perf_counter()
        logger.info("Фоновое обновление сигналов началось")

        sem = asyncio.Semaphore(ANALYZE_CONCURRENCY)
        tasks = []
        task_keys = []

        for symbol in DEFAULT_SYMBOLS:
            for timeframe in SCAN_TIMEFRAMES:
                for duration_type in SCAN_DURATION_TYPES:
                    tasks.append(run_bounded_analysis(symbol, timeframe, duration_type, sem))
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

            elapsed = round(time.perf_counter() - started_at, 3)
            logger.info(
                "Сигналы обновлены: cache=%s, scan_cache=%s, scanned=%s, active=%s, history=%s, elapsed=%ss",
                len(signal_cache),
                len(scan_cache),
                len(all_results),
                len(active_signals),
                len(signal_history),
                elapsed,
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
    try:
        init_postgres()
        logger.info("POSTGRES INIT OK")
        load_state_from_postgres()
        logger.info("STATE LOADED FROM POSTGRES")
    except Exception as e:
        logger.exception("POSTGRES INIT/LOAD FAILED: %s", e)
        active_signals.clear()
        signal_history.clear()

    deduplicate_active_signals()
    deduplicate_signal_history()

    try:
        update_closed_history_results()
        update_waiting_history_results()
        save_state()
    except Exception as e:
        logger.exception("Ошибка подготовки состояния: %s", e)

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
        "active_expire_grace_seconds": ACTIVE_EXPIRE_GRACE_SECONDS,
        "analyze_concurrency": ANALYZE_CONCURRENCY,
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
        "active_expire_grace_seconds": ACTIVE_EXPIRE_GRACE_SECONDS,
        "analyze_concurrency": ANALYZE_CONCURRENCY,
    }


@app.get("/health/db")
def health_db():
    conn = None
    cur = None

    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS count FROM active_signals_pg;")
        active_count = cur.fetchone()["count"]

        cur.execute("SELECT COUNT(*) AS count FROM signal_history_pg;")
        history_count = cur.fetchone()["count"]

        return {
            "status": "ok",
            "database_url_set": bool(DATABASE_URL),
            "active_signals_pg_count": active_count,
            "signal_history_pg_count": history_count,
        }
    except Exception as e:
        logger.exception("DB HEALTH FAILED: %s", e)
        return {
            "status": "error",
            "database_url_set": bool(DATABASE_URL),
            "detail": str(e),
        }
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


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
def get_active_signals(limit: int = 300):
    safe_reconcile_for_api(update_waiting=False)

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

        if (now_utc - exit_dt).total_seconds() >= ACTIVE_EXPIRE_GRACE_SECONDS:
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
        "active_expire_grace_seconds": ACTIVE_EXPIRE_GRACE_SECONDS,
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
def get_history(limit: int = 500):
    safe_reconcile_for_api(update_waiting=True)

    history_items = [
        item for item in signal_history
        if item.get("result") in ("WAITING_RESULT", "TP", "SL")
    ]

    def sort_dt(item: dict) -> datetime:
        value = (
            item.get("exit_time_iso")
            or item.get("closed_at_iso")
            or item.get("entry_time_iso")
            or ""
        )
        try:
            return parse_iso_utc(value)
        except Exception:
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

    waiting_items = sorted(
        [item for item in history_items if item.get("result") == "WAITING_RESULT"],
        key=sort_dt,
        reverse=True,
    )

    final_items = sorted(
        [item for item in history_items if item.get("result") in ("TP", "SL")],
        key=sort_dt,
        reverse=True,
    )

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
        "active_expire_grace_seconds": ACTIVE_EXPIRE_GRACE_SECONDS,
        "waiting_retry_seconds": WAITING_RETRY_SECONDS,
        "analyze_concurrency": ANALYZE_CONCURRENCY,
    }


@app.get("/feed")
def get_feed():
    return build_feed()

try:
    from news_feed import build_feed
except ImportError:
    def build_feed() -> dict:
        return {
            "hero": None,
            "top_news": [],
            "forex_news": [],
            "market_pulse": [],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict

import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query

from signal_engine import analyze_symbol
from news_context import get_news_context, news_settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("autosignal-api")

DATABASE_URL = os.environ.get("DATABASE_URL")
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY")
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
MARKET_DATA_API_KEY = MASSIVE_API_KEY or POLYGON_API_KEY
MARKET_DATA_BASE_URL = os.environ.get(
    "MARKET_DATA_BASE_URL",
    "https://api.massive.com" if MASSIVE_API_KEY else "https://api.polygon.io",
)
MARKET_DATA_PROVIDER = "massive" if MASSIVE_API_KEY else "polygon"

if not DATABASE_URL:
    logger.warning("DATABASE_URL not set")

if not MARKET_DATA_API_KEY:
    logger.warning("No real-time market data API key set (MASSIVE_API_KEY or POLYGON_API_KEY)")


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

MIN_CONFIDENCE_TO_KEEP = float(os.environ.get("MIN_CONFIDENCE_TO_KEEP", "55"))
MIN_VOLATILITY_RATIO = float(os.environ.get("MIN_VOLATILITY_RATIO", "0.75"))
REQUIRE_AT_LEAST_ONE_BIAS_MATCH = os.environ.get(
    "REQUIRE_AT_LEAST_ONE_BIAS_MATCH", "true"
).lower() == "true"
REQUIRE_REAL_ANALYSIS_SOURCE = os.environ.get(
    "REQUIRE_REAL_ANALYSIS_SOURCE", "true"
).lower() == "true"
LIVE_QUOTE_CACHE_SECONDS = 5
LIVE_QUOTE_MAX_AGE_SECONDS = int(os.environ.get("LIVE_QUOTE_MAX_AGE_SECONDS", "60"))
EXPERIENCE_MIN_SAMPLES = int(os.environ.get("EXPERIENCE_MIN_SAMPLES", "30"))
EXPERIENCE_PRIOR_SAMPLES = float(os.environ.get("EXPERIENCE_PRIOR_SAMPLES", "20"))
EXPERIENCE_MAX_ADJUSTMENT = float(os.environ.get("EXPERIENCE_MAX_ADJUSTMENT", "0.10"))
REAL_ANALYSIS_SOURCES = {"massive_aggregates", "polygon_aggregates"}
NEWS_MAX_CONFIDENCE_ADJUSTMENT = float(
    os.environ.get("NEWS_MAX_CONFIDENCE_ADJUSTMENT", "5")
)

signal_cache: Dict[str, Dict[str, Any]] = {}
scan_cache: Dict[str, Dict[str, Any]] = {}
last_updated_at: str | None = None
last_refresh_status: str = "starting"

active_signals: list[dict] = []
signal_history: list[dict] = []

refresh_lock = asyncio.Lock()
live_quote_cache: Dict[str, tuple[float, dict]] = {}


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


def prune_legacy_active_signals() -> int:
    """Drop forecasts created before real analysis and live-quote validation."""
    global active_signals

    before = len(active_signals)
    active_signals = [
        item
        for item in active_signals
        if item.get("analysis_data_source") in REAL_ANALYSIS_SOURCES
        and item.get("confidence_type") == "heuristic_signal_strength"
        and item.get("price_source")
        and item.get("live_quote_time_iso")
        and item.get("strategy_signals")
    ]
    removed = before - len(active_signals)
    if removed:
        logger.info("PRUNED LEGACY ACTIVE SIGNALS: %s", removed)
    return removed


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


def has_real_entry(item: dict) -> bool:
    return (
        safe_float(item.get("entry_price")) is not None
        and bool(item.get("entry_price_source"))
        and bool(item.get("entry_quote_time_iso"))
    )


def has_real_exit(item: dict) -> bool:
    return (
        safe_float(item.get("exit_price")) is not None
        and bool(item.get("exit_price_source"))
        and bool(item.get("exit_quote_time_iso"))
    )


def has_verified_execution(item: dict) -> bool:
    return has_real_entry(item) and has_real_exit(item)


def has_verified_analysis(item: dict) -> bool:
    return (
        item.get("analysis_data_source") in REAL_ANALYSIS_SOURCES
        and item.get("confidence_type") == "heuristic_signal_strength"
    )


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


def get_real_quote(symbol: str, target_dt: datetime, signal: str, phase: str) -> dict | None:
    """Return an executable forex Bid/Ask close to target_dt, never a candle price."""
    if not MARKET_DATA_API_KEY or signal not in ("BUY", "SELL"):
        return None

    pair = symbol.replace("=X", "").replace("-", "")
    ticker = f"C:{pair}"
    window = timedelta(minutes=2)
    target_ns = int(target_dt.timestamp() * 1_000_000_000)

    try:
        response = requests.get(
            f"{MARKET_DATA_BASE_URL}/v3/quotes/{ticker}",
            params={
                "timestamp.gte": int((target_dt - window).timestamp() * 1_000_000_000),
                "timestamp.lte": int((target_dt + window).timestamp() * 1_000_000_000),
                "order": "asc",
                "sort": "timestamp",
                "limit": 50000,
                "apiKey": MARKET_DATA_API_KEY,
            },
            timeout=15,
        )
        response.raise_for_status()
        quotes = response.json().get("results", [])

        candidates = []
        for quote in quotes:
            timestamp_ns = quote.get("participant_timestamp") or quote.get("sip_timestamp") or quote.get("t")
            bid = safe_float(quote.get("bid_price"))
            ask = safe_float(quote.get("ask_price"))
            if timestamp_ns is None or bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
                continue
            distance_ns = abs(int(timestamp_ns) - target_ns)
            if distance_ns <= int(window.total_seconds() * 1_000_000_000):
                candidates.append((distance_ns, int(timestamp_ns), bid, ask))

        if not candidates:
            return None

        # The quote prevailing at the requested instant is the latest quote at
        # or before that instant. Using a later quote would leak future price
        # information into the recorded entry or expiry result.
        quotes_at_or_before = [value for value in candidates if value[1] <= target_ns]
        if quotes_at_or_before:
            _, timestamp_ns, bid, ask = max(quotes_at_or_before, key=lambda value: value[1])
        else:
            _, timestamp_ns, bid, ask = min(candidates, key=lambda value: value[1])
        if phase == "entry":
            execution_price = ask if signal == "BUY" else bid
        else:
            execution_price = bid if signal == "BUY" else ask

        return {
            "price": round(execution_price, 5),
            "bid": round(bid, 5),
            "ask": round(ask, 5),
            "spread": round(ask - bid, 5),
            "timestamp_iso": datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc).isoformat(),
            "provider": MARKET_DATA_PROVIDER,
        }
    except Exception as error:
        logger.warning("REAL QUOTE FAILED %s %s: %s", symbol, phase, error)
        return None


def get_current_market_quote(symbol: str) -> dict | None:
    cached = live_quote_cache.get(symbol)
    now_monotonic = time.monotonic()
    if cached and now_monotonic - cached[0] <= LIVE_QUOTE_CACHE_SECONDS:
        return dict(cached[1])

    pair = symbol.replace("=X", "").replace("-", "")
    if not MARKET_DATA_API_KEY or len(pair) != 6:
        return None

    quote = None
    try:
        response = requests.get(
            f"{MARKET_DATA_BASE_URL}/v1/last_quote/currencies/{pair[:3]}/{pair[3:]}",
            params={"apiKey": MARKET_DATA_API_KEY},
            timeout=10,
        )
        response.raise_for_status()
        last = response.json().get("last") or {}
        bid = safe_float(last.get("bid"))
        ask = safe_float(last.get("ask"))
        timestamp_ms = last.get("timestamp")
        if bid is not None and ask is not None and timestamp_ms is not None and bid > 0 and ask >= bid:
            quote_time = datetime.fromtimestamp(float(timestamp_ms) / 1000, tz=timezone.utc)
            age_seconds = abs((datetime.now(timezone.utc) - quote_time).total_seconds())
            if age_seconds <= LIVE_QUOTE_MAX_AGE_SECONDS:
                quote = {
                    "price": round(ask, 5),
                    "bid": round(bid, 5),
                    "ask": round(ask, 5),
                    "spread": round(ask - bid, 5),
                    "timestamp_iso": quote_time.isoformat(),
                    "provider": f"{MARKET_DATA_PROVIDER}_last_quote",
                }
            else:
                logger.info("STALE LIVE QUOTE %s age_seconds=%.1f", symbol, age_seconds)
    except Exception as error:
        logger.warning("LAST QUOTE FAILED %s: %s", symbol, error)

    # Some plans may not expose the dedicated last-quote endpoint. Fall back to
    # the closest BBO quote only when it is still fresh enough to be called live.
    if quote is None:
        quote = get_real_quote(symbol, datetime.now(timezone.utc), "BUY", "entry")
        if quote is not None:
            try:
                quote_time = parse_iso_utc(quote["timestamp_iso"])
                age_seconds = abs((datetime.now(timezone.utc) - quote_time).total_seconds())
                if age_seconds > LIVE_QUOTE_MAX_AGE_SECONDS:
                    logger.info("STALE FALLBACK QUOTE %s age_seconds=%.1f", symbol, age_seconds)
                    quote = None
            except Exception:
                quote = None

    if quote is None:
        return None

    quote = dict(quote)
    quote["mid"] = round((quote["bid"] + quote["ask"]) / 2, 5)
    live_quote_cache[symbol] = (now_monotonic, quote)
    return dict(quote)


def get_latest_exit_price_for_item(item: dict) -> float | None:
    exit_time_iso = item.get("exit_time_iso")
    symbol = item.get("symbol")
    signal = item.get("signal")
    if not exit_time_iso or not symbol:
        return None

    try:
        exit_dt = parse_iso_utc(exit_time_iso)
    except Exception:
        return None

    quote = get_real_quote(symbol, exit_dt, signal, "exit")
    if quote is None:
        return None

    item["exit_bid"] = quote["bid"]
    item["exit_ask"] = quote["ask"]
    item["exit_spread"] = quote["spread"]
    item["exit_quote_time_iso"] = quote["timestamp_iso"]
    item["exit_price_source"] = quote["provider"]
    return quote["price"]


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

    if entry_price is None or signal not in ("BUY", "SELL") or not has_real_entry(item):
        item["result"] = "WAITING_RESULT"
        item["outcome"] = None
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
        return item

    try:
        entry_price = float(entry_price)
    except Exception:
        item["result"] = "WAITING_RESULT"
        item["outcome"] = None
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
        return item

    if exit_price is None:
        exit_price = get_latest_exit_price_for_item(item)

    if exit_price is None:
        item["result"] = "WAITING_RESULT"
        item["outcome"] = None
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
        return item

    try:
        exit_price = float(exit_price)
    except Exception:
        item["result"] = "WAITING_RESULT"
        item["outcome"] = None
        item["exit_price"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
        return item

    item["exit_price"] = round(exit_price, 5)

    if not has_real_exit(item):
        item["result"] = "WAITING_RESULT"
        item["outcome"] = None
        item["profit_value"] = None
        item["profit_percent"] = None
        item["last_fact_retry_iso"] = now_iso()
        return item

    if signal == "BUY":
        profit_value = exit_price - entry_price
        item["result"] = "TP" if exit_price >= entry_price else "SL"
    else:
        profit_value = entry_price - exit_price
        item["result"] = "TP" if exit_price <= entry_price else "SL"

    profit_percent = (profit_value / entry_price * 100) if entry_price else 0.0

    item["profit_value"] = round(profit_value, 5)
    item["profit_percent"] = round(profit_percent, 3)
    item["outcome"] = "WIN" if profit_value >= 0 else "LOSS"

    return item


def calculate_experience_adjustment(item: dict) -> dict:
    """Return a conservative, past-only strategy adjustment.

    The Bayesian prior contributes an equal number of virtual wins and losses,
    preventing a short lucky streak from dominating a forecast. Only records
    with verified executable entry and exit quotes are eligible.
    """
    signal = item.get("signal")
    strategy_names = {
        str(strategy.get("name"))
        for strategy in item.get("strategy_signals", [])
        if strategy.get("signal") == signal and strategy.get("name")
    }
    if not strategy_names:
        return {"factor": 1.0, "samples": 0, "posterior_win_rate": None}

    wins = 0
    losses = 0
    for historical in signal_history:
        if not has_verified_execution(historical) or not has_verified_analysis(historical):
            continue
        if historical.get("timeframe") != item.get("timeframe"):
            continue
        if historical.get("duration_type") != item.get("duration_type"):
            continue
        if (
            item.get("market_regime")
            and historical.get("market_regime")
            and historical.get("market_regime") != item.get("market_regime")
        ):
            continue

        historical_names = {
            str(strategy.get("name"))
            for strategy in historical.get("strategy_signals", [])
            if strategy.get("name")
        }
        if not strategy_names.intersection(historical_names):
            continue

        outcome = historical.get("outcome")
        if outcome == "WIN" or (outcome is None and historical.get("result") == "TP"):
            wins += 1
        elif outcome == "LOSS" or (outcome is None and historical.get("result") == "SL"):
            losses += 1

    samples = wins + losses
    if samples < EXPERIENCE_MIN_SAMPLES:
        return {"factor": 1.0, "samples": samples, "posterior_win_rate": None}

    prior_wins = EXPERIENCE_PRIOR_SAMPLES / 2
    posterior_win_rate = (wins + prior_wins) / (samples + EXPERIENCE_PRIOR_SAMPLES)
    raw_adjustment = (posterior_win_rate - 0.5) * 0.5
    bounded_adjustment = max(
        -EXPERIENCE_MAX_ADJUSTMENT,
        min(EXPERIENCE_MAX_ADJUSTMENT, raw_adjustment),
    )
    return {
        "factor": round(1.0 + bounded_adjustment, 4),
        "samples": samples,
        "posterior_win_rate": round(posterior_win_rate, 4),
    }


def apply_news_context(item: dict, context: dict) -> dict:
    """Use news only as a bounded confirmation; never create or flip a signal."""
    item = dict(item)
    status = str(context.get("status") or "disabled")
    news_score = safe_float(context.get("score")) or 0.0
    signal = item.get("signal")
    adjustment = 0.0

    if status == "ok" and signal in ("BUY", "SELL"):
        directional_support = news_score if signal == "BUY" else -news_score
        adjustment = max(
            -NEWS_MAX_CONFIDENCE_ADJUSTMENT,
            min(NEWS_MAX_CONFIDENCE_ADJUSTMENT, directional_support * NEWS_MAX_CONFIDENCE_ADJUSTMENT),
        )
        confidence = safe_float(item.get("confidence")) or 0.0
        item["confidence"] = round(max(0.0, min(90.0, confidence + adjustment)), 1)

        if abs(news_score) >= 0.08:
            relation = "подтверждают" if directional_support > 0 else "противоречат"
            suffix = f"Новости {relation} направлению ({news_score:+.2f})"
            current_reason = str(item.get("reason") or "").strip()
            item["reason"] = f"{current_reason}; {suffix}" if current_reason else suffix

    item["news_status"] = status
    item["news_provider"] = context.get("provider")
    item["news_score"] = round(news_score, 4)
    item["news_direction"] = context.get("direction") or "NEUTRAL"
    item["news_article_count"] = int(context.get("article_count") or 0)
    item["news_items"] = context.get("items") or []
    item["news_updated_at_iso"] = context.get("updated_at_iso")
    item["news_confidence_adjustment"] = round(adjustment, 2)
    return item


def add_signals_to_active(items: list[dict]) -> None:
    global active_signals

    existing_keys = {make_signal_key(s) for s in active_signals}

    for item in items:
        experience = calculate_experience_adjustment(item)
        try:
            confidence_value = float(item.get("confidence", 50) or 50)
        except Exception:
            confidence_value = 50.0

        confidence_value *= experience["factor"]
        item["confidence"] = round(max(1.0, min(90.0, confidence_value)), 1)
        item["experience_samples"] = experience["samples"]
        item["experience_win_rate"] = experience["posterior_win_rate"]
        item["experience_factor"] = experience["factor"]

        signal = item.get("signal")
        if signal not in ("BUY", "SELL"):
            continue

        analysis_source = str(item.get("analysis_data_source") or "")
        if REQUIRE_REAL_ANALYSIS_SOURCE and analysis_source not in REAL_ANALYSIS_SOURCES:
            logger.info(
                "SIGNAL FILTERED BY DATA SOURCE: %s %s source=%s",
                item.get("symbol"),
                item.get("timeframe"),
                analysis_source,
            )
            continue

        if (
            item.get("live_bid") is None
            or item.get("live_ask") is None
            or not item.get("live_quote_time_iso")
            or not item.get("price_source")
        ):
            logger.info(
                "SIGNAL FILTERED BY MISSING LIVE QUOTE: %s %s",
                item.get("symbol"),
                item.get("timeframe"),
            )
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
                "analysis_price": item.get("price"),
                "entry_price": None,
                "entry_bid": None,
                "entry_ask": None,
                "entry_spread": None,
                "entry_quote_time_iso": None,
                "entry_price_source": None,
                "status": "active",
                "closed_at_iso": None,
                "result": "OPEN",
                "outcome": None,
            }
        )

        existing_keys.add(key)


def capture_due_entry_prices() -> bool:
    now_utc = datetime.now(timezone.utc)
    changed = False

    for item in active_signals:
        if item.get("result") != "OPEN" or has_real_entry(item):
            continue

        try:
            entry_dt = parse_iso_utc(item.get("entry_time_iso", ""))
        except Exception:
            continue

        if now_utc < entry_dt:
            continue

        quote = get_real_quote(item.get("symbol", ""), entry_dt, item.get("signal", ""), "entry")
        if quote is None:
            continue

        item["entry_price"] = quote["price"]
        item["entry_bid"] = quote["bid"]
        item["entry_ask"] = quote["ask"]
        item["entry_spread"] = quote["spread"]
        item["entry_quote_time_iso"] = quote["timestamp_iso"]
        item["entry_price_source"] = quote["provider"]

        analysis_price = safe_float(item.get("analysis_price")) or safe_float(item.get("price"))
        old_tp = safe_float(item.get("tp"))
        old_sl = safe_float(item.get("sl"))
        if analysis_price is not None:
            if old_tp is not None:
                item["tp"] = round(quote["price"] + (old_tp - analysis_price), 5)
            if old_sl is not None:
                item["sl"] = round(quote["price"] + (old_sl - analysis_price), 5)
        changed = True

    if changed:
        save_state()
    return changed


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
        close_reason = "real_bid_ask_quote"

        closed_item = finalize_closed_signal(
            item,
            exit_price=exit_price,
            close_reason=close_reason if exit_price is not None else "real_exit_quote_not_found",
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
        capture_due_entry_prices()
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

        indicator_price = safe_float(result.get("price"))
        result["indicator_price"] = indicator_price
        live_quote = await asyncio.to_thread(get_current_market_quote, symbol)
        if live_quote is not None:
            live_mid = live_quote["mid"]
            old_tp = safe_float(result.get("tp"))
            old_sl = safe_float(result.get("sl"))
            if indicator_price is not None:
                if old_tp is not None:
                    result["tp"] = round(live_mid + (old_tp - indicator_price), 5)
                if old_sl is not None:
                    result["sl"] = round(live_mid + (old_sl - indicator_price), 5)
            result["price"] = live_mid
            result["live_bid"] = live_quote["bid"]
            result["live_ask"] = live_quote["ask"]
            result["live_spread"] = live_quote["spread"]
            result["live_quote_time_iso"] = live_quote["timestamp_iso"]
            result["price_source"] = live_quote["provider"]
        else:
            result["live_bid"] = None
            result["live_ask"] = None
            result["live_spread"] = None
            result["live_quote_time_iso"] = None
            result["price_source"] = None

        news_context = await asyncio.to_thread(get_news_context, symbol)
        result = apply_news_context(result, news_context)

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
            capture_due_entry_prices()
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
    prune_legacy_active_signals()
    deduplicate_signal_history()

    try:
        update_closed_history_results()
        update_waiting_history_results()
        save_state()
    except Exception as e:
        logger.exception("Ошибка подготовки состояния: %s", e)

    # Do not block the ASGI startup on 64 network-heavy analyses. Render needs
    # the port to open quickly; the loop performs its first refresh immediately.
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


def public_signal_settings() -> dict:
    return {
        "min_confidence": MIN_CONFIDENCE_TO_KEEP,
        "min_volatility_ratio": MIN_VOLATILITY_RATIO,
        "require_bias_match": REQUIRE_AT_LEAST_ONE_BIAS_MATCH,
        "require_real_analysis_source": REQUIRE_REAL_ANALYSIS_SOURCE,
        "live_quote_max_age_seconds": LIVE_QUOTE_MAX_AGE_SECONDS,
        "experience_min_samples": EXPERIENCE_MIN_SAMPLES,
        "experience_prior_samples": EXPERIENCE_PRIOR_SAMPLES,
        "experience_max_adjustment": EXPERIENCE_MAX_ADJUSTMENT,
        "news_max_confidence_adjustment": NEWS_MAX_CONFIDENCE_ADJUSTMENT,
        "news": news_settings(),
        "confidence_type": "heuristic_signal_strength",
    }


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
        "signal_settings": public_signal_settings(),
    }


@app.get("/settings")
def get_settings():
    return public_signal_settings()


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
        if (
            item.get("result") == "WAITING_RESULT" and has_real_entry(item)
        ) or (
            item.get("result") in ("TP", "SL") and has_verified_execution(item)
        )
    ]

    legacy_items_count = len(signal_history) - len(history_items)

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
        "verified_execution_only": True,
        "legacy_items_excluded": legacy_items_count,
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

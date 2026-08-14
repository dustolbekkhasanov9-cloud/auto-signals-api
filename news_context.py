from datetime import datetime, timedelta, timezone
from typing import Any
import logging
import math
import os
import threading
import time

import requests


logger = logging.getLogger("news-context")

ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
NEWS_ENABLED = (
    bool(ALPHA_VANTAGE_API_KEY)
    and os.environ.get("NEWS_ANALYSIS_ENABLED", "true").lower() == "true"
)
NEWS_API_URL = os.environ.get("NEWS_API_URL", "https://www.alphavantage.co/query")
NEWS_CACHE_SECONDS = int(os.environ.get("NEWS_CACHE_SECONDS", "1800"))
NEWS_LOOKBACK_HOURS = int(os.environ.get("NEWS_LOOKBACK_HOURS", "12"))
NEWS_HALF_LIFE_HOURS = float(os.environ.get("NEWS_HALF_LIFE_HOURS", "3"))
NEWS_MIN_RELEVANCE = float(os.environ.get("NEWS_MIN_RELEVANCE", "0.20"))
NEWS_MAX_ITEMS = int(os.environ.get("NEWS_MAX_ITEMS", "5"))

_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_inflight: dict[str, threading.Event] = {}
_lock = threading.Lock()


def _request_error_summary(error: Exception) -> str:
    response = getattr(error, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code is not None:
        return f"{type(error).__name__} status={status_code}"
    return type(error).__name__


def news_settings() -> dict[str, Any]:
    return {
        "enabled": NEWS_ENABLED,
        "provider": "alpha_vantage" if NEWS_ENABLED else None,
        "cache_seconds": NEWS_CACHE_SECONDS,
        "lookback_hours": NEWS_LOOKBACK_HOURS,
        "half_life_hours": NEWS_HALF_LIFE_HOURS,
        "min_relevance": NEWS_MIN_RELEVANCE,
    }


def _empty_context(symbol: str, status: str, reason: str = "") -> dict[str, Any]:
    return {
        "symbol": symbol,
        "status": status,
        "reason": reason,
        "provider": "alpha_vantage" if NEWS_ENABLED else None,
        "score": 0.0,
        "direction": "NEUTRAL",
        "article_count": 0,
        "items": [],
        "updated_at_iso": datetime.now(timezone.utc).isoformat(),
    }


def _parse_news_time(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ticker_sentiment(article: dict[str, Any], ticker: str) -> tuple[float | None, float]:
    for item in article.get("ticker_sentiment") or []:
        if str(item.get("ticker") or "").upper() != ticker:
            continue
        score = _safe_float(item.get("ticker_sentiment_score"))
        relevance = _safe_float(item.get("relevance_score")) or 0.0
        return score, max(0.0, min(1.0, relevance))
    return None, 0.0


def _build_context(symbol: str, articles: list[dict[str, Any]]) -> dict[str, Any]:
    pair = symbol.replace("=X", "").replace("-", "").upper()
    if len(pair) != 6:
        return _empty_context(symbol, "unsupported_symbol", "Expected a six-letter Forex pair")

    base_ticker = f"FOREX:{pair[:3]}"
    quote_ticker = f"FOREX:{pair[3:]}"
    now_utc = datetime.now(timezone.utc)
    weighted_sum = 0.0
    total_weight = 0.0
    scored_items: list[dict[str, Any]] = []

    for article in articles:
        published_at = _parse_news_time(article.get("time_published"))
        if published_at is None:
            continue
        age_hours = max((now_utc - published_at).total_seconds() / 3600, 0.0)
        if age_hours > NEWS_LOOKBACK_HOURS:
            continue

        base_score, base_relevance = _ticker_sentiment(article, base_ticker)
        quote_score, quote_relevance = _ticker_sentiment(article, quote_ticker)
        if base_score is None and quote_score is None:
            continue

        if base_score is not None and quote_score is not None:
            pair_score = (base_score - quote_score) / 2
        elif base_score is not None:
            pair_score = base_score
        else:
            pair_score = -float(quote_score)

        relevance = max(base_relevance, quote_relevance)
        if relevance < NEWS_MIN_RELEVANCE:
            continue

        recency_weight = math.pow(0.5, age_hours / max(NEWS_HALF_LIFE_HOURS, 0.1))
        weight = relevance * recency_weight
        weighted_sum += pair_score * weight
        total_weight += weight
        scored_items.append(
            {
                "title": str(article.get("title") or "")[:300],
                "url": article.get("url"),
                "source": article.get("source"),
                "published_at_iso": published_at.isoformat(),
                "pair_score": round(max(-1.0, min(1.0, pair_score)), 4),
                "weight": round(weight, 4),
            }
        )

    if total_weight <= 0:
        return _empty_context(symbol, "no_relevant_news")

    score = max(-1.0, min(1.0, weighted_sum / total_weight))
    direction = "BUY" if score >= 0.08 else "SELL" if score <= -0.08 else "NEUTRAL"
    scored_items.sort(key=lambda item: item["weight"], reverse=True)

    return {
        "symbol": symbol,
        "status": "ok",
        "reason": "",
        "provider": "alpha_vantage",
        "score": round(score, 4),
        "direction": direction,
        "article_count": len(scored_items),
        "items": scored_items[:NEWS_MAX_ITEMS],
        "updated_at_iso": now_utc.isoformat(),
    }


def get_news_context(symbol: str) -> dict[str, Any]:
    if not NEWS_ENABLED:
        return _empty_context(symbol, "disabled", "ALPHA_VANTAGE_API_KEY is not configured")

    now_monotonic = time.monotonic()
    with _lock:
        cached = _cache.get(symbol)
        if cached and now_monotonic - cached[0] <= NEWS_CACHE_SECONDS:
            return dict(cached[1])

        wait_event = _inflight.get(symbol)
        if wait_event is None:
            wait_event = threading.Event()
            _inflight[symbol] = wait_event
            request_owner = True
        else:
            request_owner = False

    if not request_owner:
        wait_event.wait(timeout=20)
        with _lock:
            cached = _cache.get(symbol)
            if cached:
                return dict(cached[1])
        return _empty_context(symbol, "temporarily_unavailable", "News request did not finish")

    result = _empty_context(symbol, "error")
    try:
        pair = symbol.replace("=X", "").replace("-", "").upper()
        if len(pair) != 6:
            result = _empty_context(symbol, "unsupported_symbol")
        else:
            response = requests.get(
                NEWS_API_URL,
                params={
                    "function": "NEWS_SENTIMENT",
                    "tickers": f"FOREX:{pair[:3]},FOREX:{pair[3:]}",
                    "time_from": (datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS)).strftime(
                        "%Y%m%dT%H%M"
                    ),
                    "sort": "LATEST",
                    "limit": 50,
                    "apikey": ALPHA_VANTAGE_API_KEY,
                },
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("Information") or payload.get("Note") or payload.get("Error Message"):
                reason = payload.get("Information") or payload.get("Note") or payload.get("Error Message")
                result = _empty_context(symbol, "provider_limit", str(reason)[:300])
            else:
                result = _build_context(symbol, payload.get("feed") or [])
    except Exception as error:
        summary = _request_error_summary(error)
        logger.warning("NEWS CONTEXT FAILED %s: %s", symbol, summary)
        result = _empty_context(symbol, "error", summary)
    finally:
        with _lock:
            _cache[symbol] = (time.monotonic(), result)
            event = _inflight.pop(symbol, None)
            if event:
                event.set()

    return dict(result)

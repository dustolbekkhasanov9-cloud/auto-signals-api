from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
import yfinance as yf

RSI_PERIOD = 14
ATR_PERIOD = 14

DEFAULT_TIMEFRAME = "1h"
DEFAULT_DURATION_TYPE = "short"

TIMEFRAME_CONFIG = {
    "5m": {"fetch_interval": "5m", "period": "5d", "resample": None},
    "10m": {"fetch_interval": "5m", "period": "5d", "resample": "10min"},
    "15m": {"fetch_interval": "15m", "period": "10d", "resample": None},
    "30m": {"fetch_interval": "30m", "period": "20d", "resample": None},
    "1h": {"fetch_interval": "60m", "period": "30d", "resample": None},
    "1d": {"fetch_interval": "1d", "period": "6mo", "resample": None},
}

TIMEFRAME_LABELS = {
    "5m": "5m",
    "10m": "10m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "1d": "1d",
}

SHORT_EXPIRY_MAP = {
    "5m": timedelta(minutes=15),
    "10m": timedelta(minutes=20),
    "15m": timedelta(minutes=30),
    "30m": timedelta(hours=1),
    "1h": timedelta(hours=2),
    "1d": timedelta(days=1),
}

LONG_EXPIRY_MAP = {
    "5m": timedelta(hours=1),
    "10m": timedelta(hours=2),
    "15m": timedelta(hours=2),
    "30m": timedelta(hours=4),
    "1h": timedelta(hours=6),
    "1d": timedelta(days=2),
}

CHART_POINTS_MAP = {
    "5m": 36,
    "10m": 36,
    "15m": 36,
    "30m": 36,
    "1h": 36,
    "1d": 30,
}


def empty_signal_payload(symbol: str, reason: str, timeframe: str = DEFAULT_TIMEFRAME, duration_type: str = DEFAULT_DURATION_TYPE) -> dict:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "duration_type": duration_type,
        "price": None,
        "entry_price": None,
        "signal": "NONE",
        "confidence": 0.0,
        "rsi": None,
        "tp": None,
        "sl": None,
        "market_regime": "UNKNOWN",
        "strategy": "Нет сигнала",
        "recommended_expiry": "",
        "chart_prices": [],
        "chart_labels": [],
        "entry_time": "Нет сигнала",
        "exit_time": "Нет сигнала",
        "entry_time_iso": "",
        "exit_time_iso": "",
        "reason": reason,
    }


def normalize_timeframe(timeframe: str | None) -> str:
    if not timeframe:
        return DEFAULT_TIMEFRAME
    timeframe = timeframe.strip().lower()
    if timeframe in TIMEFRAME_CONFIG:
        return timeframe
    return DEFAULT_TIMEFRAME


def normalize_duration_type(duration_type: str | None) -> str:
    if not duration_type:
        return DEFAULT_DURATION_TYPE
    value = duration_type.strip().lower()
    if value in ("short", "long"):
        return value
    return DEFAULT_DURATION_TYPE


def period_to_days(period: str) -> int:
    if period.endswith("d"):
        return max(int(period[:-1]), 1)
    if period.endswith("mo"):
        return max(int(period[:-2]) * 30, 30)
    return 30


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    ohlcv = pd.DataFrame()
    ohlcv["Open"] = df["Open"].resample(rule).first()
    ohlcv["High"] = df["High"].resample(rule).max()
    ohlcv["Low"] = df["Low"].resample(rule).min()
    ohlcv["Close"] = df["Close"].resample(rule).last()
    if "Volume" in df.columns:
        ohlcv["Volume"] = df["Volume"].resample(rule).sum()
    else:
        ohlcv["Volume"] = 0
    ohlcv.dropna(inplace=True)
    return ohlcv


def fetch_data(symbol: str, timeframe: str = DEFAULT_TIMEFRAME) -> pd.DataFrame | None:
    timeframe = normalize_timeframe(timeframe)
    cfg = TIMEFRAME_CONFIG[timeframe]
    fetch_interval = cfg["fetch_interval"]
    period = cfg["period"]
    days = period_to_days(period)

    try:
        period1 = int((datetime.utcnow() - timedelta(days=days)).timestamp())
        period2 = int(datetime.utcnow().timestamp())
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{symbol}?period1={period1}&period2={period2}&interval={fetch_interval}"
        )
        headers = {"User-Agent": "Mozilla/5.0"}

        r = requests.get(url, headers=headers, timeout=12)
        data = r.json()

        result = data["chart"]["result"][0]
        ts = result["timestamp"]
        quote = result["indicators"]["quote"][0]

        df = pd.DataFrame(quote)
        df["Date"] = pd.to_datetime(ts, unit="s", utc=True)
        df.set_index("Date", inplace=True)
        df.rename(
            columns={
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            },
            inplace=True,
        )
        df.dropna(inplace=True)

        if cfg["resample"]:
            df = resample_ohlcv(df, cfg["resample"])

        return df if not df.empty else None

    except Exception:
        try:
            df = yf.download(
                symbol,
                period=period,
                interval=fetch_interval,
                progress=False,
                threads=False,
                auto_adjust=True,
            )
            if df is None or df.empty:
                return None

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

            df = df.rename(columns=lambda c: str(c).capitalize())

            for col in ["Open", "High", "Low", "Close"]:
                if col not in df.columns:
                    return None

            if "Volume" not in df.columns:
                df["Volume"] = 0

            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")

            df.dropna(inplace=True)

            if cfg["resample"]:
                df = resample_ohlcv(df, cfg["resample"])

            return df if not df.empty else None
        except Exception:
            return None


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def compute_rsi(series: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))


def compute_atr(df: pd.DataFrame, period: int = ATR_PERIOD) -> pd.Series:
    prev_close = df["Close"].shift(1)
    tr1 = df["High"] - df["Low"]
    tr2 = (df["High"] - prev_close).abs()
    tr3 = (df["Low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean().bfill()


def compute_macd(series: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    macd_line = ema(series, 12) - ema(series, 26)
    signal_line = ema(macd_line, 9)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def compute_bollinger(series: pd.Series, period: int = 20, num_std: float = 2.0) -> tuple[pd.Series, pd.Series, pd.Series]:
    mid = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = mid + num_std * std
    lower = mid - num_std * std
    return lower, mid, upper


def compute_heikin_ashi(df: pd.DataFrame) -> pd.DataFrame:
    ha = pd.DataFrame(index=df.index)
    ha["HA_Close"] = (df["Open"] + df["High"] + df["Low"] + df["Close"]) / 4

    ha_open = [(df["Open"].iloc[0] + df["Close"].iloc[0]) / 2]
    for i in range(1, len(df)):
        ha_open.append((ha_open[i - 1] + ha["HA_Close"].iloc[i - 1]) / 2)

    ha["HA_Open"] = ha_open
    ha["HA_High"] = pd.concat([df["High"], ha["HA_Open"], ha["HA_Close"]], axis=1).max(axis=1)
    ha["HA_Low"] = pd.concat([df["Low"], ha["HA_Open"], ha["HA_Close"]], axis=1).min(axis=1)
    return ha


def detect_market_regime(df: pd.DataFrame) -> str:
    last = df.iloc[-1]
    ema20 = float(last["EMA20"])
    ema50 = float(last["EMA50"])
    atr = float(last["ATR"])
    price = float(last["Close"])

    ema_gap = abs(ema20 - ema50)
    atr_ratio = atr / price if price else 0.0

    if ema_gap < atr * 0.35:
        return "FLAT"
    if ema20 > ema50 and atr_ratio >= 0.0015:
        return "UPTREND"
    if ema20 < ema50 and atr_ratio >= 0.0015:
        return "DOWNTREND"
    return "RANGE"


def make_strategy_result(name: str, signal: str = "NONE", score: float = 0.0, reasons: list[str] | None = None) -> dict[str, Any]:
    return {
        "name": name,
        "signal": signal,
        "score": float(score),
        "reasons": reasons or [],
    }


def strategy_ema_pullback(df: pd.DataFrame) -> dict[str, Any]:
    last = df.iloc[-1]
    prev = df.iloc[-2]

    price = float(last["Close"])
    prev_price = float(prev["Close"])
    ema20 = float(last["EMA20"])
    ema50 = float(last["EMA50"])
    prev_ema20 = float(prev["EMA20"])

    if ema20 > ema50 and price > ema20 and prev_price <= prev_ema20:
        return make_strategy_result(
            "EMA Pullback",
            "BUY",
            18,
            ["EMA20 выше EMA50", "Цена вернулась выше EMA20", "Есть подтверждение продолжения вверх"],
        )

    if ema20 < ema50 and price < ema20 and prev_price >= prev_ema20:
        return make_strategy_result(
            "EMA Pullback",
            "SELL",
            18,
            ["EMA20 ниже EMA50", "Цена вернулась ниже EMA20", "Есть подтверждение продолжения вниз"],
        )

    return make_strategy_result("EMA Pullback")


def strategy_rsi_reversal(df: pd.DataFrame) -> dict[str, Any]:
    last = df.iloc[-1]
    prev = df.iloc[-2]

    rsi = float(last["RSI"])
    prev_rsi = float(prev["RSI"])
    price = float(last["Close"])
    prev_price = float(prev["Close"])

    if prev_rsi < 30 and rsi > prev_rsi and price > prev_price:
        return make_strategy_result(
            "RSI Reversal",
            "BUY",
            14,
            [f"RSI выходит из перепроданности ({rsi:.1f})", "Цена начала разворачиваться вверх"],
        )

    if prev_rsi > 70 and rsi < prev_rsi and price < prev_price:
        return make_strategy_result(
            "RSI Reversal",
            "SELL",
            14,
            [f"RSI выходит из перекупленности ({rsi:.1f})", "Цена начала разворачиваться вниз"],
        )

    return make_strategy_result("RSI Reversal")


def strategy_bollinger_reversal(df: pd.DataFrame) -> dict[str, Any]:
    last = df.iloc[-1]
    prev = df.iloc[-2]

    price = float(last["Close"])
    prev_price = float(prev["Close"])
    bb_lower = float(last["BB_LOWER"])
    bb_upper = float(last["BB_UPPER"])

    if prev_price < bb_lower and price > prev_price:
        return make_strategy_result(
            "Bollinger Reversal",
            "BUY",
            12,
            ["Цена вышла ниже нижней полосы Bollinger", "Есть возврат вверх"],
        )

    if prev_price > bb_upper and price < prev_price:
        return make_strategy_result(
            "Bollinger Reversal",
            "SELL",
            12,
            ["Цена вышла выше верхней полосы Bollinger", "Есть возврат вниз"],
        )

    return make_strategy_result("Bollinger Reversal")


def strategy_macd_momentum(df: pd.DataFrame) -> dict[str, Any]:
    last = df.iloc[-1]
    prev = df.iloc[-2]

    macd = float(last["MACD"])
    macd_signal = float(last["MACD_SIGNAL"])
    prev_macd = float(prev["MACD"])
    prev_signal = float(prev["MACD_SIGNAL"])

    if prev_macd <= prev_signal and macd > macd_signal:
        return make_strategy_result(
            "MACD Momentum",
            "BUY",
            15,
            ["MACD пересёк сигнальную линию вверх", "Импульс усиливается"],
        )

    if prev_macd >= prev_signal and macd < macd_signal:
        return make_strategy_result(
            "MACD Momentum",
            "SELL",
            15,
            ["MACD пересёк сигнальную линию вниз", "Импульс ослабевает"],
        )

    return make_strategy_result("MACD Momentum")


def strategy_breakout(df: pd.DataFrame) -> dict[str, Any]:
    if len(df) < 25:
        return make_strategy_result("Breakout")

    last = df.iloc[-1]
    recent = df.iloc[-21:-1]

    price = float(last["Close"])
    atr = float(last["ATR"])
    recent_high = float(recent["High"].max())
    recent_low = float(recent["Low"].min())

    if price > recent_high + atr * 0.05:
        return make_strategy_result(
            "Breakout",
            "BUY",
            20,
            ["Цена пробила локальный максимум", "Пробой подтверждён волатильностью"],
        )

    if price < recent_low - atr * 0.05:
        return make_strategy_result(
            "Breakout",
            "SELL",
            20,
            ["Цена пробила локальный минимум", "Пробой подтверждён волатильностью"],
        )

    return make_strategy_result("Breakout")


def strategy_heikin_ashi(df: pd.DataFrame) -> dict[str, Any]:
    if len(df) < 4:
        return make_strategy_result("Heikin Ashi")

    last3 = df.tail(3)

    bullish = all(last3["HA_Close"] > last3["HA_Open"])
    bearish = all(last3["HA_Close"] < last3["HA_Open"])

    if bullish:
        return make_strategy_result(
            "Heikin Ashi Trend",
            "BUY",
            10,
            ["Три подряд бычьих Heikin Ashi свечи", "Краткосрочный тренд вверх подтверждён"],
        )

    if bearish:
        return make_strategy_result(
            "Heikin Ashi Trend",
            "SELL",
            10,
            ["Три подряд медвежьих Heikin Ashi свечи", "Краткосрочный тренд вниз подтверждён"],
        )

    return make_strategy_result("Heikin Ashi Trend")


def strategy_support_resistance(df: pd.DataFrame) -> dict[str, Any]:
    if len(df) < 30:
        return make_strategy_result("Support/Resistance")

    last = df.iloc[-1]
    recent = df.iloc[-25:-1]

    price = float(last["Close"])
    low = float(last["Low"])
    high = float(last["High"])
    atr = float(last["ATR"])

    support = float(recent["Low"].min())
    resistance = float(recent["High"].max())

    if abs(low - support) <= atr * 0.25 and price > support:
        return make_strategy_result(
            "Support Bounce",
            "BUY",
            11,
            ["Цена тестирует поддержку", "Есть отскок от уровня"],
        )

    if abs(high - resistance) <= atr * 0.25 and price < resistance:
        return make_strategy_result(
            "Resistance Bounce",
            "SELL",
            11,
            ["Цена тестирует сопротивление", "Есть отбой вниз от уровня"],
        )

    return make_strategy_result("Support/Resistance")


def build_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["RSI"] = compute_rsi(df["Close"], RSI_PERIOD)
    df["ATR"] = compute_atr(df, ATR_PERIOD)
    df["EMA20"] = ema(df["Close"], 20)
    df["EMA50"] = ema(df["Close"], 50)

    macd_line, signal_line, macd_hist = compute_macd(df["Close"])
    df["MACD"] = macd_line
    df["MACD_SIGNAL"] = signal_line
    df["MACD_HIST"] = macd_hist

    bb_lower, bb_mid, bb_upper = compute_bollinger(df["Close"], 20, 2.0)
    df["BB_LOWER"] = bb_lower
    df["BB_MID"] = bb_mid
    df["BB_UPPER"] = bb_upper

    ha = compute_heikin_ashi(df)
    df["HA_Open"] = ha["HA_Open"]
    df["HA_Close"] = ha["HA_Close"]
    df["HA_High"] = ha["HA_High"]
    df["HA_Low"] = ha["HA_Low"]

    df.dropna(inplace=True)
    return df


def round_time_for_timeframe(now_utc: datetime, timeframe: str) -> datetime:
    base = now_utc.replace(second=0, microsecond=0)

    if timeframe == "5m":
        minute = ((base.minute // 5) + 1) * 5
        if minute >= 60:
            return base.replace(minute=0) + timedelta(hours=1)
        return base.replace(minute=minute)

    if timeframe == "10m":
        minute = ((base.minute // 10) + 1) * 10
        if minute >= 60:
            return base.replace(minute=0) + timedelta(hours=1)
        return base.replace(minute=minute)

    if timeframe == "15m":
        minute = ((base.minute // 15) + 1) * 15
        if minute >= 60:
            return base.replace(minute=0) + timedelta(hours=1)
        return base.replace(minute=minute)

    if timeframe == "30m":
        minute = ((base.minute // 30) + 1) * 30
        if minute >= 60:
            return base.replace(minute=0) + timedelta(hours=1)
        return base.replace(minute=minute)

    if timeframe == "1h":
        return base.replace(minute=0) + timedelta(hours=1)

    if timeframe == "1d":
        return base.replace(hour=0, minute=0) + timedelta(days=1)

    return base.replace(minute=0) + timedelta(hours=1)


def get_expiry_delta(timeframe: str, duration_type: str) -> timedelta:
    timeframe = normalize_timeframe(timeframe)
    duration_type = normalize_duration_type(duration_type)

    if duration_type == "long":
        return LONG_EXPIRY_MAP.get(timeframe, timedelta(hours=6))
    return SHORT_EXPIRY_MAP.get(timeframe, timedelta(hours=2))


def format_expiry_label(delta: timedelta) -> str:
    total_minutes = int(delta.total_seconds() // 60)
    if total_minutes < 60:
        return f"{total_minutes}m"
    if total_minutes < 1440:
        hours = total_minutes // 60
        return f"{hours}h"
    days = total_minutes // 1440
    return f"{days}d"


def combine_strategy_results(results: list[dict[str, Any]], market_regime: str) -> tuple[str, float, str, str]:
    buy_score = sum(float(r["score"]) for r in results if r["signal"] == "BUY")
    sell_score = sum(float(r["score"]) for r in results if r["signal"] == "SELL")

    if market_regime == "FLAT":
        buy_score *= 0.88
        sell_score *= 0.88

    if buy_score > sell_score:
        signal = "BUY"
        winning_score = buy_score
        losing_score = sell_score
        winning_strategies = [r["name"] for r in results if r["signal"] == "BUY"]
        winning_reasons = [reason for r in results if r["signal"] == "BUY" for reason in r["reasons"]]
    elif sell_score > buy_score:
        signal = "SELL"
        winning_score = sell_score
        losing_score = buy_score
        winning_strategies = [r["name"] for r in results if r["signal"] == "SELL"]
        winning_reasons = [reason for r in results if r["signal"] == "SELL" for reason in r["reasons"]]
    else:
        return "NONE", 0.0, "Нет сигнала", "Недостаточно оснований"

    spread = max(winning_score - losing_score, 0.0)
    confidence = min(round(winning_score + spread * 0.35, 1), 95.0)

    strategy_name = " + ".join(winning_strategies[:3]) if winning_strategies else "Нет сигнала"
    reason_text = "; ".join(dict.fromkeys(winning_reasons)) if winning_reasons else "Нет оснований"

    return signal, confidence, strategy_name, reason_text


def analyze_symbol(symbol: str, timeframe: str = DEFAULT_TIMEFRAME, duration_type: str = DEFAULT_DURATION_TYPE) -> dict:
    timeframe = normalize_timeframe(timeframe)
    duration_type = normalize_duration_type(duration_type)

    df = fetch_data(symbol, timeframe=timeframe)
    if df is None or len(df) < 80:
        return empty_signal_payload(symbol, "Недостаточно данных", timeframe, duration_type)

    df = build_indicators(df)
    if df is None or df.empty or len(df) < 60:
        return empty_signal_payload(symbol, "Недостаточно данных после расчёта индикаторов", timeframe, duration_type)

    last = df.iloc[-1]
    price = float(last["Close"])
    rsi = float(last["RSI"])
    atr = float(last["ATR"])
    market_regime = detect_market_regime(df)

    strategy_results = [
        strategy_ema_pullback(df),
        strategy_rsi_reversal(df),
        strategy_bollinger_reversal(df),
        strategy_macd_momentum(df),
        strategy_breakout(df),
        strategy_heikin_ashi(df),
        strategy_support_resistance(df),
    ]

    signal, confidence, strategy_name, reason = combine_strategy_results(strategy_results, market_regime)

    if signal == "NONE":
        entry_price = None
        tp = None
        sl = None
        entry_time = "Нет сигнала"
        exit_time = "Нет сигнала"
        entry_time_iso = ""
        exit_time_iso = ""
        recommended_expiry = ""
    else:
        now_utc = datetime.now(timezone.utc)
        entry_dt_utc = round_time_for_timeframe(now_utc, timeframe)
        expiry_delta = get_expiry_delta(timeframe, duration_type)
        exit_dt_utc = entry_dt_utc + expiry_delta

        entry_time = entry_dt_utc.strftime("%H:%M")
        exit_time = exit_dt_utc.strftime("%H:%M")

        entry_time_iso = entry_dt_utc.isoformat().replace("+00:00", "Z")
        exit_time_iso = exit_dt_utc.isoformat().replace("+00:00", "Z")
        recommended_expiry = format_expiry_label(expiry_delta)

        entry_price = price

        rr_multiplier = 1.8 if duration_type == "short" else 2.2
        sl_multiplier = 1.0 if duration_type == "short" else 1.2

        if signal == "BUY":
            tp = price + atr * rr_multiplier
            sl = price - atr * sl_multiplier
        else:
            tp = price - atr * rr_multiplier
            sl = price + atr * sl_multiplier

    chart_points = CHART_POINTS_MAP.get(timeframe, 36)
    chart_df = df.tail(chart_points)

    if timeframe == "1d":
        chart_labels = [idx.strftime("%d.%m") for idx in chart_df.index]
    else:
        chart_labels = [idx.strftime("%d %H:%M") for idx in chart_df.index]

    chart_prices = [round(float(x), 5) for x in chart_df["Close"].tolist()]

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "duration_type": duration_type,
        "price": price,
        "entry_price": entry_price,
        "signal": signal,
        "confidence": confidence,
        "rsi": round(rsi, 2),
        "tp": round(float(tp), 5) if tp is not None else None,
        "sl": round(float(sl), 5) if sl is not None else None,
        "market_regime": market_regime,
        "strategy": strategy_name,
        "recommended_expiry": recommended_expiry,
        "chart_prices": chart_prices,
        "chart_labels": chart_labels,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_time_iso": entry_time_iso,
        "exit_time_iso": exit_time_iso,
        "reason": reason,
    }
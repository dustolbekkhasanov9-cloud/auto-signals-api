from datetime import datetime, timedelta
import requests
import pandas as pd
import yfinance as yf

PRIMARY_TF = "1h"
RSI_PERIOD = 14
ATR_PERIOD = 14


def fetch_data(symbol: str, days: int = 30, interval: str = PRIMARY_TF) -> pd.DataFrame | None:
    try:
        period1 = int((datetime.utcnow() - timedelta(days=days)).timestamp())
        period2 = int(datetime.utcnow().timestamp())
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{symbol}?period1={period1}&period2={period2}&interval={interval}"
        )
        headers = {"User-Agent": "Mozilla/5.0"}

        r = requests.get(url, headers=headers, timeout=12)
        data = r.json()

        result = data["chart"]["result"][0]
        ts = result["timestamp"]
        quote = result["indicators"]["quote"][0]

        df = pd.DataFrame(quote)
        df["Date"] = pd.to_datetime(ts, unit="s")
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
        return df

    except Exception:
        try:
            df = yf.download(
                symbol,
                period=f"{days}d",
                interval=interval,
                progress=False,
                threads=False,
                auto_adjust=True,
            )
            if df is None or df.empty:
                return None
            df = df.rename(columns=lambda c: c.capitalize())
            df.dropna(inplace=True)
            return df
        except Exception:
            return None


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, 1e-10)
    return 100 - (100 / (1 + rs))


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["Close"].shift(1)
    tr1 = df["High"] - df["Low"]
    tr2 = (df["High"] - prev_close).abs()
    tr3 = (df["Low"] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(period).mean().bfill()


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def analyze_symbol(symbol: str) -> dict:
    df = fetch_data(symbol)
    if df is None or len(df) < 80:
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
            "reason": "Недостаточно данных",
        }

    df["RSI"] = compute_rsi(df["Close"], RSI_PERIOD)
    df["ATR"] = compute_atr(df, ATR_PERIOD)
    df["EMA20"] = ema(df["Close"], 20)
    df["EMA50"] = ema(df["Close"], 50)

    last = df.iloc[-1]
    prev = df.iloc[-2]

    price = float(last["Close"])
    prev_price = float(prev["Close"])
    rsi = float(last["RSI"])
    atr = float(last["ATR"])
    ema20 = float(last["EMA20"])
    ema50 = float(last["EMA50"])

    score_buy = 0
    score_sell = 0
    reasons = []

    if ema20 > ema50:
        score_buy += 30
        reasons.append("EMA20 выше EMA50")
    else:
        score_sell += 30
        reasons.append("EMA20 ниже EMA50")

    if price > ema20:
        score_buy += 20
        reasons.append("Цена выше EMA20")
    else:
        score_sell += 20
        reasons.append("Цена ниже EMA20")

    if price > prev_price:
        score_buy += 10
        reasons.append("Последняя свеча растущая")
    elif price < prev_price:
        score_sell += 10
        reasons.append("Последняя свеча падающая")

    if 45 <= rsi <= 65 and ema20 > ema50 and price > ema20:
        score_buy += 15
        reasons.append(f"RSI поддерживает рост ({rsi:.1f})")
    elif 35 <= rsi <= 55 and ema20 < ema50 and price < ema20:
        score_sell += 15
        reasons.append(f"RSI поддерживает падение ({rsi:.1f})")

    prev_ema20 = float(prev["EMA20"])
    if price > ema20 and prev_price <= prev_ema20:
        score_buy += 15
        reasons.append("Пробой EMA20 вверх")
    elif price < ema20 and prev_price >= prev_ema20:
        score_sell += 15
        reasons.append("Пробой EMA20 вниз")

    ema_gap = abs(ema20 - ema50)

    if ema_gap < atr * 0.3:
        market_regime = "FLAT"
    elif ema20 > ema50:
        market_regime = "UPTREND"
    else:
        market_regime = "DOWNTREND"

    if score_buy > score_sell:
        signal = "BUY"
        raw_confidence = score_buy
    elif score_sell > score_buy:
        signal = "SELL"
        raw_confidence = score_sell
    else:
        signal = "NONE"
        raw_confidence = 0

    confidence = min(round(raw_confidence, 1), 95.0)

    if signal == "NONE":
        entry_price = None
        tp = None
        sl = None
        entry_time = "Нет сигнала"
        exit_time = "Нет сигнала"
    else:
        now = datetime.now()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        entry_time = next_hour.strftime("%H:%M")
        exit_time = (next_hour + timedelta(hours=2)).strftime("%H:%M")

        # цена входа: текущая цена как базовый ориентир стратегии
        entry_price = price

        if signal == "BUY":
            tp = price + atr * 1.5
            sl = price - atr
        else:
            tp = price - atr * 1.5
            sl = price + atr

    chart_df = df.tail(24)
    chart_prices = [round(float(x), 5) for x in chart_df["Close"].tolist()]
    chart_labels = [idx.strftime("%d %H:%M") for idx in chart_df.index]

    return {
        "symbol": symbol,
        "price": round(price, 5),
        "entry_price": round(entry_price, 5) if entry_price is not None else None,
        "signal": signal,
        "confidence": confidence,
        "rsi": round(rsi, 1),
        "tp": round(tp, 5) if tp is not None else None,
        "sl": round(sl, 5) if sl is not None else None,
        "market_regime": market_regime,
        "chart_prices": chart_prices,
        "chart_labels": chart_labels,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "reason": "; ".join(reasons),
    }
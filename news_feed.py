import requests
from datetime import datetime, timezone

NEWS_API_KEY = "PASTE_YOUR_KEY_HERE"


def fetch_top_news():
    url = "https://newsapi.org/v2/top-headlines"

    params = {
        "category": "business",
        "language": "en",
        "pageSize": 5,
        "apiKey": NEWS_API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()

    articles = []

    for item in data.get("articles", []):
        articles.append({
            "title": item.get("title"),
            "summary": item.get("description"),
            "image_url": item.get("urlToImage"),
            "source": item.get("source", {}).get("name"),
            "url": item.get("url"),
            "published_at": item.get("publishedAt")
        })

    return articles


def fetch_forex_news():
    url = "https://newsapi.org/v2/everything"

    params = {
        "q": "forex OR usd OR eur OR fed OR ecb",
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 10,
        "apiKey": NEWS_API_KEY
    }

    r = requests.get(url, params=params)
    data = r.json()

    articles = []

    for item in data.get("articles", []):
        articles.append({
            "title": item.get("title"),
            "summary": item.get("description"),
            "image_url": item.get("urlToImage"),
            "source": item.get("source", {}).get("name"),
            "url": item.get("url"),
            "published_at": item.get("publishedAt")
        })

    return articles


def fetch_market_pulse():
    return [
        {"symbol": "EURUSD=X", "title": "EUR/USD"},
        {"symbol": "GBPUSD=X", "title": "GBP/USD"},
        {"symbol": "USDJPY=X", "title": "USD/JPY"},
        {"symbol": "AUDUSD=X", "title": "AUD/USD"},
        {"symbol": "USDCHF=X", "title": "USD/CHF"},
        {"symbol": "USDCAD=X", "title": "USD/CAD"},
        {"symbol": "NZDUSD=X", "title": "NZD/USD"},
        {"symbol": "EURJPY=X", "title": "EUR/JPY"}
    ]


def build_feed():

    top_news = fetch_top_news()
    forex_news = fetch_forex_news()
    market_pulse = fetch_market_pulse()

    hero = top_news[0] if top_news else None

    return {
        "hero": hero,
        "top_news": top_news,
        "forex_news": forex_news,
        "market_pulse": market_pulse,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

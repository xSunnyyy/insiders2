"""Stocktwits scraper.

Two endpoints are useful:
  - trending symbols:  /streams/trending.json
  - per-symbol stream: /streams/symbol/{TICKER}.json

Each message may carry an explicit `entities.sentiment.basic` value of
"Bullish" or "Bearish" assigned by the poster. We use that directly when
present and fall back to lexicon scoring otherwise.
"""

from __future__ import annotations

import logging
import time

import requests

LOG = logging.getLogger(__name__)

BASE = "https://api.stocktwits.com/api/2"
UA = "stock-sentiment-bot/0.1"


def _get(url: str) -> dict | None:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=10)
        if r.status_code != 200:
            LOG.warning("stocktwits %s -> %s", url, r.status_code)
            return None
        return r.json()
    except requests.RequestException as e:
        LOG.warning("stocktwits request failed: %s", e)
        return None


def trending_symbols(limit: int = 30) -> list[str]:
    data = _get(f"{BASE}/streams/trending.json")
    if not data:
        return []
    syms: list[str] = []
    seen: set[str] = set()
    for msg in data.get("messages", []):
        for sym in msg.get("symbols", []) or []:
            s = (sym.get("symbol") or "").upper()
            if s and s not in seen:
                seen.add(s)
                syms.append(s)
                if len(syms) >= limit:
                    return syms
    return syms


def fetch_symbol_stream(symbol: str) -> list[dict]:
    """Return messages for a given ticker. Each item carries explicit
    sentiment when the poster tagged it."""
    data = _get(f"{BASE}/streams/symbol/{symbol}.json")
    if not data:
        return []
    out: list[dict] = []
    for msg in data.get("messages", []):
        ent = msg.get("entities") or {}
        sent = (ent.get("sentiment") or {}).get("basic")  # 'Bullish' | 'Bearish' | None
        out.append({
            "title": "",
            "body": msg.get("body", "") or "",
            "score": 1,
            "url": f"https://stocktwits.com/symbol/{symbol}",
            "source": "stocktwits",
            "explicit_sentiment": sent,
            "symbols": [s.get("symbol", "").upper()
                        for s in (msg.get("symbols") or [])
                        if s.get("symbol")],
        })
    return out


def fetch_all(max_symbols: int = 25) -> list[dict]:
    """Fetch messages for the top trending symbols on Stocktwits."""
    syms = trending_symbols(limit=max_symbols)
    LOG.info("stocktwits: trending = %s", syms)
    items: list[dict] = []
    for s in syms:
        items.extend(fetch_symbol_stream(s))
        time.sleep(0.4)
    LOG.info("stocktwits: collected %d messages across %d symbols",
             len(items), len(syms))
    return items

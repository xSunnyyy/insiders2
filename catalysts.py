"""Per-ticker catalyst lookup: upcoming earnings + recent news.

Sources:
  - Earnings:  Yahoo Finance v10/finance/quoteSummary?modules=calendarEvents
  - News:      Yahoo Finance v1/finance/search?q=<TICKER>&newsCount=N

Both are unauthenticated. Each call is cheap; we only run them for the
top-N tickers after ranking, in parallel. Failures degrade silently.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

LOG = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

EARNINGS_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
NEWS_URL = "https://query2.finance.yahoo.com/v1/finance/search"


def _earnings_for(symbol: str) -> dict | None:
    sym = symbol.replace(".", "-")
    try:
        r = requests.get(
            EARNINGS_URL.format(symbol=sym),
            params={"modules": "calendarEvents"},
            headers={"User-Agent": UA}, timeout=8,
        )
        if r.status_code != 200:
            return None
        result = ((r.json() or {}).get("quoteSummary") or {}).get("result") or []
        if not result:
            return None
        ev = (result[0].get("calendarEvents") or {}).get("earnings") or {}
        dates = ev.get("earningsDate") or []
        if not dates:
            return None
        # earningsDate is a list of {raw, fmt}; take the soonest in the future.
        upcoming = [d for d in dates if d.get("raw")]
        if not upcoming:
            return None
        nearest = min(upcoming, key=lambda d: d["raw"])
        ts = int(nearest["raw"])
        days_out = (ts - int(time.time())) // 86400
        return {
            "earnings_date": datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat(),
            "earnings_days_out": int(days_out),
        }
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        LOG.debug("earnings fetch failed for %s: %s", symbol, e)
        return None


def _news_for(symbol: str, n: int = 3) -> list[dict]:
    sym = symbol.replace(".", "-")
    try:
        r = requests.get(
            NEWS_URL,
            params={"q": sym, "newsCount": n, "quotesCount": 0,
                    "enableFuzzyQuery": "false"},
            headers={"User-Agent": UA}, timeout=8,
        )
        if r.status_code != 200:
            return []
        items = (r.json() or {}).get("news") or []
        out: list[dict] = []
        for it in items[:n]:
            ts = int(it.get("providerPublishTime") or 0)
            out.append({
                "title": it.get("title") or "",
                "publisher": it.get("publisher") or "",
                "ts": ts,
                "age_hours": (int(time.time()) - ts) // 3600 if ts else None,
                "url": it.get("link") or "",
            })
        return out
    except (requests.RequestException, ValueError, TypeError) as e:
        LOG.debug("news fetch failed for %s: %s", symbol, e)
        return []


def _one(symbol: str) -> dict:
    out: dict = {"earnings_date": None, "earnings_days_out": None, "news": []}
    e = _earnings_for(symbol)
    if e:
        out.update(e)
    out["news"] = _news_for(symbol, n=3)
    return out


def fetch(symbols: list[str], max_workers: int = 8) -> dict[str, dict]:
    """Return {symbol: {earnings_date, earnings_days_out, news[]}}."""
    out: dict[str, dict] = {}
    if not symbols:
        return out
    started = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_one, s): s for s in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                out[sym] = fut.result()
            except Exception as e:
                LOG.debug("catalyst worker failed for %s: %s", sym, e)
                out[sym] = {"earnings_date": None,
                            "earnings_days_out": None, "news": []}
    LOG.info("catalysts: %d/%d in %.2fs", len(out), len(symbols),
             time.time() - started)
    return out


def summary_label(c: dict) -> str:
    """Short string suitable for a table cell."""
    parts: list[str] = []
    if c.get("earnings_days_out") is not None:
        d = c["earnings_days_out"]
        if d == 0:
            parts.append("ER today")
        elif 0 < d <= 14:
            parts.append(f"ER in {d}d")
        elif -3 <= d < 0:
            parts.append(f"ER {abs(d)}d ago")
    news = c.get("news") or []
    if news:
        h = news[0]
        age = h.get("age_hours")
        tag = f" ({age}h)" if age is not None and age < 72 else ""
        parts.append(h["title"][:80] + tag)
    return " | ".join(parts) if parts else ""

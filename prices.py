"""Live quote fetcher.

Primary source: Yahoo Finance v8 chart endpoint (includes pre/post-market).
Fallback: Stooq CSV (free, no key, datacenter-friendly).

Yahoo's v8 chart endpoint USUALLY works without auth, but in 2024-25 it
started rejecting some datacenter IPs (returns 401 / empty result). We
attach the crumb when we have one; if Yahoo still gives nothing, we fall
back to Stooq which returns the same fields from a different host.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import yahoo_auth

LOG = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
STOOQ_URL = "https://stooq.com/q/l/"


def _yahoo(symbol: str) -> dict | None:
    yahoo_sym = symbol.replace(".", "-")
    try:
        session, crumb = yahoo_auth.get()
        params = {"interval": "1d", "range": "2d", "includePrePost": "true"}
        if crumb:
            params["crumb"] = crumb
        r = session.get(YAHOO_URL.format(symbol=yahoo_sym),
                        params=params, timeout=8)
        if r.status_code != 200:
            return None
        result = ((r.json() or {}).get("chart") or {}).get("result") or []
        if not result:
            return None
        meta = result[0].get("meta") or {}
        price = meta.get("regularMarketPrice")
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        if price is None or prev is None or prev == 0:
            return None
        change_pct = (price - prev) / prev * 100.0
        pre = meta.get("preMarketPrice")
        pre_chg = meta.get("preMarketChangePercent")
        post = meta.get("postMarketPrice")
        post_chg = meta.get("postMarketChangePercent")
        return {
            "price": round(float(price), 2),
            "prev_close": round(float(prev), 2),
            "change_pct": round(float(change_pct), 2),
            "currency": meta.get("currency", ""),
            "pre_price": round(float(pre), 2) if pre is not None else None,
            "pre_change_pct": round(float(pre_chg), 2) if pre_chg is not None else None,
            "post_price": round(float(post), 2) if post is not None else None,
            "post_change_pct": round(float(post_chg), 2) if post_chg is not None else None,
            "source": "yahoo",
        }
    except (requests.RequestException, ValueError, TypeError) as e:
        LOG.debug("yahoo fetch failed for %s: %s", symbol, e)
        return None


def _stooq(symbol: str) -> dict | None:
    """Stooq fallback. Returns price + change_pct but not pre/post-market.

    Stooq uses suffixes for the exchange: US stocks are `<ticker>.us`.
    Their CSV daily endpoint at /q/l/?s=AAPL.US&i=d returns:
        Date,Open,High,Low,Close,Volume
    fetched for the last 2 sessions when `range=d` and we pass the
    interval flag. The closest "two consecutive sessions" approach is
    to ask for `i=d&l=2` (or the historical CSV).
    """
    stooq_sym = symbol.replace(".", "-").lower() + ".us"
    try:
        r = requests.get(
            STOOQ_URL,
            params={"s": stooq_sym, "f": "sd2t2ohlcv", "h": "", "e": "csv"},
            headers={"User-Agent": UA}, timeout=8,
        )
        if r.status_code != 200 or not r.text:
            return None
        reader = csv.DictReader(io.StringIO(r.text))
        row = next(reader, None)
        if not row:
            return None
        close = row.get("Close")
        if close in (None, "", "N/D"):
            return None
        price = float(close)
        open_ = float(row.get("Open") or 0) or None
        # Stooq's intraday quote doesn't include yesterday's close. Approximate
        # change% off today's open if we have nothing better.
        change_pct = None
        if open_:
            change_pct = round((price - open_) / open_ * 100.0, 2)
        return {
            "price": round(price, 2),
            "prev_close": round(open_, 2) if open_ else None,
            "change_pct": change_pct,
            "currency": "USD",
            "pre_price": None, "pre_change_pct": None,
            "post_price": None, "post_change_pct": None,
            "source": "stooq",
        }
    except (requests.RequestException, ValueError, TypeError, KeyError) as e:
        LOG.debug("stooq fetch failed for %s: %s", symbol, e)
        return None


def _fetch_one(symbol: str) -> dict | None:
    # Stocktwits emits crypto with a ".X" suffix (e.g. "BTC.X"). Those
    # won't resolve to equities on either source -- skip cheaply.
    if symbol.endswith(".X") or symbol.endswith("-X"):
        return None
    out = _yahoo(symbol)
    if out:
        return out
    return _stooq(symbol)


def fetch_quotes(symbols: list[str], max_workers: int = 12) -> dict[str, dict]:
    """Return {symbol: {price, prev_close, change_pct, currency, pre/post}}.
    Missing symbols are simply absent."""
    out: dict[str, dict] = {}
    if not symbols:
        return out
    started = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_fetch_one, s): s for s in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            q = fut.result()
            if q:
                out[sym] = q
    LOG.info("prices: %d/%d quotes in %.2fs (yahoo=%d, stooq=%d)",
             len(out), len(symbols), time.time() - started,
             sum(1 for v in out.values() if v.get("source") == "yahoo"),
             sum(1 for v in out.values() if v.get("source") == "stooq"))
    return out

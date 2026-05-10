"""Live quote fetcher.

Stocktwits' free public API does not return prices, so we use Yahoo Finance's
v8 chart endpoint (`query1.finance.yahoo.com/v8/finance/chart/<symbol>`),
which is unauthenticated and gives us `regularMarketPrice` and
`chartPreviousClose` -- enough to compute change %.

One request per ticker; we only call this for the top-N tickers after
ranking, so the request count is bounded.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

LOG = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
QUOTE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


def _fetch_one(symbol: str) -> dict | None:
    try:
        # The Berkshire-style "BRK.B" symbol must be sent as "BRK-B" to Yahoo.
        yahoo_sym = symbol.replace(".", "-")
        r = requests.get(
            QUOTE_URL.format(symbol=yahoo_sym),
            params={"interval": "1d", "range": "2d"},
            headers={"User-Agent": UA},
            timeout=8,
        )
        if r.status_code != 200:
            LOG.debug("yahoo %s -> %s", symbol, r.status_code)
            return None
        data = r.json()
        result = ((data.get("chart") or {}).get("result") or [])
        if not result:
            return None
        meta = result[0].get("meta") or {}
        price = meta.get("regularMarketPrice")
        prev = meta.get("chartPreviousClose") or meta.get("previousClose")
        if price is None or prev is None or prev == 0:
            return None
        change_pct = (price - prev) / prev * 100.0
        return {
            "price": round(float(price), 2),
            "prev_close": round(float(prev), 2),
            "change_pct": round(float(change_pct), 2),
            "currency": meta.get("currency", ""),
        }
    except (requests.RequestException, ValueError, TypeError) as e:
        LOG.debug("yahoo fetch failed for %s: %s", symbol, e)
        return None


def fetch_quotes(symbols: list[str], max_workers: int = 8) -> dict[str, dict]:
    """Return {symbol: {price, prev_close, change_pct, currency}} for the
    symbols Yahoo recognized. Missing symbols are simply absent."""
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
    LOG.info("prices: %d/%d quotes in %.2fs",
             len(out), len(symbols), time.time() - started)
    return out

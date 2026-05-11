"""Per-ticker sector lookup using Yahoo's quoteSummary `assetProfile`.

In-process cache; sectors don't change often. We only call this for the
top-N tickers per refresh.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

import yahoo_auth

LOG = logging.getLogger(__name__)

URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"

# {symbol: (sector, industry, marketcap, fetched_at)}
_cache: dict[str, tuple[str, str, float | None, float]] = {}
CACHE_TTL_SEC = 24 * 3600  # sectors don't churn; refresh once a day


def _fetch_one(symbol: str) -> tuple[str, str, float | None] | None:
    sym = symbol.replace(".", "-")
    try:
        session, crumb = yahoo_auth.get()
        params = {"modules": "assetProfile,price,summaryDetail"}
        if crumb:
            params["crumb"] = crumb
        r = session.get(URL.format(symbol=sym), params=params, timeout=8)
        if r.status_code != 200:
            return None
        result = ((r.json() or {}).get("quoteSummary") or {}).get("result") or []
        if not result:
            return None
        ap = result[0].get("assetProfile") or {}
        pr = result[0].get("price") or {}
        sector = (ap.get("sector") or "").strip()
        industry = (ap.get("industry") or "").strip()
        mcap_obj = pr.get("marketCap") or {}
        mcap = mcap_obj.get("raw") if isinstance(mcap_obj, dict) else None
        return sector, industry, (float(mcap) if mcap is not None else None)
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        LOG.debug("sector fetch failed for %s: %s", symbol, e)
        return None


def fetch(symbols: list[str], max_workers: int = 8) -> dict[str, dict]:
    """Return {symbol: {sector, industry, market_cap}}.
    Uses cache; only hits Yahoo for entries missing or expired."""
    out: dict[str, dict] = {}
    now = time.time()
    missing: list[str] = []
    for s in symbols:
        c = _cache.get(s)
        if c and (now - c[3]) < CACHE_TTL_SEC:
            out[s] = {"sector": c[0], "industry": c[1], "market_cap": c[2]}
        else:
            missing.append(s)

    if missing:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_fetch_one, s): s for s in missing}
            for fut in as_completed(futures):
                sym = futures[fut]
                res = fut.result()
                if res is None:
                    out[sym] = {"sector": "", "industry": "", "market_cap": None}
                    continue
                sector, industry, mcap = res
                _cache[sym] = (sector, industry, mcap, now)
                out[sym] = {"sector": sector, "industry": industry,
                            "market_cap": mcap}
    return out

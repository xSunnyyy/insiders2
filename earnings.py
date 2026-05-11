"""Earnings calendar over a curated 'universe' of popular tickers.

Approach (free + no signup):
  - Maintain a list of widely-traded US tickers
  - On demand, parallel-fetch earnings dates via catalysts._earnings_for
  - Cache for 6h in memory; on Vercel a cold start rebuilds the cache,
    but warm functions reuse it.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import catalysts
from tickers import KNOWN_TICKERS

LOG = logging.getLogger(__name__)

# Universe to scan: the curated KNOWN_TICKERS, capped so a Vercel cold
# start can rebuild the cache within the function budget.
UNIVERSE = sorted(KNOWN_TICKERS)[:200]
CACHE_TTL_SEC = 6 * 3600
_cache: dict = {"items": None, "fetched_at": 0.0}


def _fetch_one(symbol: str) -> dict | None:
    e = catalysts._earnings_for(symbol)
    if not e:
        return None
    return {"symbol": symbol, **e}


def refresh(workers: int = 12) -> list[dict]:
    started = time.time()
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_one, s): s for s in UNIVERSE}
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                out.append(r)
    out.sort(key=lambda r: r.get("earnings_days_out", 9999))
    _cache["items"] = out
    _cache["fetched_at"] = time.time()
    LOG.info("earnings: %d/%d in %.2fs", len(out), len(UNIVERSE),
             time.time() - started)
    return out


def upcoming(days: int = 14) -> list[dict]:
    if _cache["items"] is None or \
            (time.time() - _cache["fetched_at"]) > CACHE_TTL_SEC:
        refresh()
    return [r for r in (_cache["items"] or [])
            if r.get("earnings_days_out") is not None
            and 0 <= r["earnings_days_out"] <= days]

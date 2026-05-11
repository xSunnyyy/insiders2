"""Earnings calendar over a curated 'universe' of popular tickers.

Approach (free + no signup):
  - Maintain a list of widely-traded US tickers
  - On demand, parallel-fetch earnings dates via catalysts._earnings_for
    (Yahoo quoteSummary with crumb)
  - Cache for 6h in memory, but only if we got results -- empty results
    retry sooner so a one-off failure doesn't pin us to [] for hours.

Diagnostics are surfaced via `last_status()` so the dashboard can
explain why a result was empty.
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
_status: dict = {"fetched": 0, "with_dates": 0, "errors": 0, "duration_sec": 0.0}


def last_status() -> dict:
    return dict(_status)


def _fetch_one(symbol: str) -> dict | None:
    try:
        e = catalysts._earnings_for(symbol)
        if not e:
            return None
        return {"symbol": symbol, **e}
    except Exception as exc:
        LOG.debug("earnings fetch failed for %s: %s", symbol, exc)
        return None


def refresh(workers: int = 12) -> list[dict]:
    started = time.time()
    fetched = with_dates = errors = 0
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_one, s): s for s in UNIVERSE}
        for fut in as_completed(futures):
            fetched += 1
            try:
                r = fut.result()
            except Exception:
                errors += 1
                continue
            if r:
                with_dates += 1
                out.append(r)
    out.sort(key=lambda r: r.get("earnings_days_out", 9999))

    _status.update({
        "fetched": fetched,
        "with_dates": with_dates,
        "errors": errors,
        "duration_sec": round(time.time() - started, 2),
        "universe_size": len(UNIVERSE),
    })

    _cache["items"] = out
    # Only honor the full TTL if we actually got something. Empty
    # results retry in ~5 minutes so a transient failure doesn't pin us.
    if out:
        _cache["fetched_at"] = time.time()
    else:
        _cache["fetched_at"] = time.time() - (CACHE_TTL_SEC - 300)

    LOG.info("earnings: %d/%d with dates in %.2fs",
             with_dates, fetched, _status["duration_sec"])
    return out


def upcoming(days: int = 14) -> list[dict]:
    if _cache["items"] is None or \
            (time.time() - _cache["fetched_at"]) > CACHE_TTL_SEC:
        refresh()
    return [r for r in (_cache["items"] or [])
            if r.get("earnings_days_out") is not None
            and 0 <= r["earnings_days_out"] <= days]

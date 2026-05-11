"""Earnings calendar.

Primary source: NASDAQ's calendar API at
    https://api.nasdaq.com/api/calendar/earnings?date=YYYY-MM-DD
which returns ALL companies reporting on a given date with EPS estimates,
market cap, and reporting time. We fan out across the requested date
window in parallel.

Fallback: scan ~200 widely-traded tickers via Yahoo quoteSummary (the
previous implementation), so the tab still works when NASDAQ is blocked
from a host's IP range.

Cache: 6h for non-empty results, 5 min for empty so transient failures
don't pin us to [].
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import requests

import catalysts
from tickers import KNOWN_TICKERS

LOG = logging.getLogger(__name__)

NASDAQ_URL = "https://api.nasdaq.com/api/calendar/earnings"
NASDAQ_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}

# Yahoo-fallback universe (the previous earnings.UNIVERSE shape).
UNIVERSE = sorted(KNOWN_TICKERS)[:200]

CACHE_TTL_SEC = 6 * 3600
_cache: dict = {"items": None, "fetched_at": 0.0, "days": 0}
_status: dict = {"source": "", "dates_fetched": 0, "rows": 0,
                 "duration_sec": 0.0, "fallback_used": False,
                 "errors": []}


def last_status() -> dict:
    return dict(_status)


# ---------------------------------------------------------------------------
# NASDAQ source
# ---------------------------------------------------------------------------

def _fetch_nasdaq_date(d: date) -> tuple[list[dict], str | None]:
    """Return (rows, error_msg). Rows are normalized to our schema."""
    try:
        r = requests.get(NASDAQ_URL,
                         params={"date": d.isoformat()},
                         headers=NASDAQ_HEADERS,
                         timeout=10)
        if r.status_code != 200:
            return [], f"{d} -> {r.status_code}"
        rows = ((r.json() or {}).get("data") or {}).get("rows") or []
        out: list[dict] = []
        today = date.today()
        for row in rows:
            sym = (row.get("symbol") or "").upper()
            if not sym:
                continue
            out.append({
                "symbol": sym,
                "name": row.get("name") or "",
                "earnings_date": d.isoformat(),
                "earnings_days_out": (d - today).days,
                "time": row.get("time") or "",        # 'time-after-hours' / 'time-pre-market' / 'time-not-supplied'
                "eps_forecast": row.get("epsForecast") or "",
                "last_year_eps": row.get("lastYearEPS") or "",
                "no_of_ests": row.get("noOfEsts") or "",
                "fiscal_quarter": row.get("fiscalQuarterEnding") or "",
                "market_cap": row.get("marketCap") or "",
            })
        return out, None
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        return [], f"{d} transport: {e}"


def _refresh_nasdaq(days: int) -> list[dict]:
    """Hit NASDAQ for each day in [today, today+days). Returns combined rows
    or [] if all dates failed."""
    today = date.today()
    dates = [today + timedelta(days=i) for i in range(days)]
    started = time.time()
    rows: list[dict] = []
    errors: list[str] = []
    dates_ok = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_fetch_nasdaq_date, d): d for d in dates}
        for fut in as_completed(futures):
            got, err = fut.result()
            if err:
                errors.append(err)
            else:
                dates_ok += 1
                rows.extend(got)
    rows.sort(key=lambda r: (r["earnings_days_out"], r["symbol"]))

    _status.update({
        "source": "nasdaq",
        "dates_fetched": dates_ok,
        "rows": len(rows),
        "duration_sec": round(time.time() - started, 2),
        "fallback_used": False,
        "errors": errors[:5],
    })
    LOG.info("earnings nasdaq: %d rows over %d dates in %.2fs (errs=%d)",
             len(rows), dates_ok, _status["duration_sec"], len(errors))
    return rows


# ---------------------------------------------------------------------------
# Yahoo fallback (per-ticker scan)
# ---------------------------------------------------------------------------

def _fetch_yahoo_one(symbol: str) -> dict | None:
    try:
        e = catalysts._earnings_for(symbol)
        if not e:
            return None
        return {"symbol": symbol, "name": "", **e,
                "time": "", "eps_forecast": "", "last_year_eps": "",
                "no_of_ests": "", "fiscal_quarter": "", "market_cap": ""}
    except Exception:
        return None


def _refresh_yahoo() -> list[dict]:
    started = time.time()
    rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(_fetch_yahoo_one, s): s for s in UNIVERSE}
        for fut in as_completed(futures):
            r = fut.result()
            if r:
                rows.append(r)
    rows.sort(key=lambda r: r.get("earnings_days_out", 9999))

    _status.update({
        "source": "yahoo-fallback",
        "dates_fetched": 0,
        "rows": len(rows),
        "duration_sec": round(time.time() - started, 2),
        "fallback_used": True,
        "errors": [],
    })
    LOG.info("earnings yahoo fallback: %d rows in %.2fs",
             len(rows), _status["duration_sec"])
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _do_refresh(days: int) -> list[dict]:
    rows = _refresh_nasdaq(days)
    if not rows:
        rows = _refresh_yahoo()
    _cache["items"] = rows
    _cache["days"] = days
    # Don't pin empty results for the full TTL.
    if rows:
        _cache["fetched_at"] = time.time()
    else:
        _cache["fetched_at"] = time.time() - (CACHE_TTL_SEC - 300)
    return rows


def upcoming(days: int = 14) -> list[dict]:
    needs_refresh = (
        _cache["items"] is None
        or (time.time() - _cache["fetched_at"]) > CACHE_TTL_SEC
        or _cache["days"] < days
    )
    if needs_refresh:
        _do_refresh(days)
    items = _cache["items"] or []
    return [r for r in items
            if r.get("earnings_days_out") is not None
            and 0 <= r["earnings_days_out"] <= days]

"""Live quote fetcher.

Three tiers tried in order:
  1. Yahoo v7/finance/quote (batched) -- one call for all symbols, uses
     the crumb session. Has pre/post-market fields inline.
  2. Yahoo v8/finance/chart (per-symbol) -- a different endpoint that
     sometimes succeeds when v7 doesn't.
  3. Stooq CSV (per-symbol, no auth) -- last-ditch fallback for hosts
     where Yahoo is blocked entirely.

Each returned dict carries `source` so the dashboard can show which path
served the row, making debugging on Vercel/Cloud hosts trivial.
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
V7_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
V8_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
STOOQ_URL = "https://stooq.com/q/l/"


# ---------------------------------------------------------------------------
# Yahoo v7 (batched)
# ---------------------------------------------------------------------------

def _yahoo_v7_batch(symbols: list[str]) -> dict[str, dict]:
    """One call for all symbols. Returns {sym: row} for what Yahoo recognized."""
    if not symbols:
        return {}
    out: dict[str, dict] = {}
    try:
        session, crumb = yahoo_auth.get()
        # Yahoo expects BRK-B not BRK.B
        params = {"symbols": ",".join(s.replace(".", "-") for s in symbols)}
        if crumb:
            params["crumb"] = crumb
        r = session.get(V7_URL, params=params, timeout=10)
        if r.status_code != 200:
            LOG.warning("yahoo v7 batch -> %s", r.status_code)
            return out
        result = ((r.json() or {}).get("quoteResponse") or {}).get("result") or []
        for row in result:
            yahoo_sym = (row.get("symbol") or "").upper()
            # Map back: BRK-B -> BRK.B (our internal form)
            our_sym = next((s for s in symbols
                            if s.replace(".", "-").upper() == yahoo_sym),
                           yahoo_sym.replace("-", "."))
            price = row.get("regularMarketPrice")
            prev = row.get("regularMarketPreviousClose") \
                or row.get("chartPreviousClose")
            change_pct = row.get("regularMarketChangePercent")
            if price is None:
                continue
            out[our_sym] = {
                "price": round(float(price), 2),
                "prev_close": round(float(prev), 2) if prev else None,
                "change_pct": round(float(change_pct), 2) if change_pct is not None else None,
                "currency": row.get("currency", "") or "",
                "pre_price": (round(float(row["preMarketPrice"]), 2)
                              if row.get("preMarketPrice") is not None else None),
                "pre_change_pct": (round(float(row["preMarketChangePercent"]), 2)
                                   if row.get("preMarketChangePercent") is not None else None),
                "post_price": (round(float(row["postMarketPrice"]), 2)
                               if row.get("postMarketPrice") is not None else None),
                "post_change_pct": (round(float(row["postMarketChangePercent"]), 2)
                                    if row.get("postMarketChangePercent") is not None else None),
                "source": "yahoo-v7",
            }
    except (requests.RequestException, ValueError, TypeError) as e:
        LOG.warning("yahoo v7 batch failed: %s", e)
    return out


# ---------------------------------------------------------------------------
# Yahoo v8 chart (per-symbol)
# ---------------------------------------------------------------------------

def _yahoo_v8(symbol: str) -> dict | None:
    yahoo_sym = symbol.replace(".", "-")
    try:
        session, crumb = yahoo_auth.get()
        params = {"interval": "1d", "range": "2d", "includePrePost": "true"}
        if crumb:
            params["crumb"] = crumb
        r = session.get(V8_URL.format(symbol=yahoo_sym),
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
        return {
            "price": round(float(price), 2),
            "prev_close": round(float(prev), 2),
            "change_pct": round(float(change_pct), 2),
            "currency": meta.get("currency", "") or "",
            "pre_price": round(float(meta["preMarketPrice"]), 2) if meta.get("preMarketPrice") is not None else None,
            "pre_change_pct": round(float(meta["preMarketChangePercent"]), 2) if meta.get("preMarketChangePercent") is not None else None,
            "post_price": round(float(meta["postMarketPrice"]), 2) if meta.get("postMarketPrice") is not None else None,
            "post_change_pct": round(float(meta["postMarketChangePercent"]), 2) if meta.get("postMarketChangePercent") is not None else None,
            "source": "yahoo-v8",
        }
    except (requests.RequestException, ValueError, TypeError) as e:
        LOG.debug("yahoo v8 fetch failed for %s: %s", symbol, e)
        return None


# ---------------------------------------------------------------------------
# Stooq (per-symbol CSV)
# ---------------------------------------------------------------------------

def _stooq(symbol: str) -> dict | None:
    stooq_sym = symbol.replace(".", "-").lower() + ".us"
    try:
        r = requests.get(
            STOOQ_URL,
            params={"s": stooq_sym, "f": "sd2t2ohlcv", "h": "", "e": "csv"},
            headers={"User-Agent": UA}, timeout=6,
        )
        if r.status_code != 200 or not r.text:
            return None
        reader = csv.DictReader(io.StringIO(r.text))
        row = next(reader, None)
        if not row or row.get("Close") in (None, "", "N/D"):
            return None
        price = float(row["Close"])
        open_ = float(row.get("Open") or 0) or None
        change_pct = round((price - open_) / open_ * 100.0, 2) if open_ else None
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


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def fetch_quotes(symbols: list[str], max_workers: int = 12) -> dict[str, dict]:
    """Three-tier fetch: v7 batch -> v8 per-symbol -> Stooq per-symbol."""
    out: dict[str, dict] = {}
    if not symbols:
        return out
    # Skip Stocktwits crypto-style suffixes early.
    eligible = [s for s in symbols
                if not s.endswith(".X") and not s.endswith("-X")]
    if not eligible:
        return out
    started = time.time()

    # Tier 1: one batched Yahoo call.
    v7 = _yahoo_v7_batch(eligible)
    out.update(v7)

    # Tier 2 + 3: only for what v7 missed.
    missing = [s for s in eligible if s not in out]
    if missing:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_yahoo_v8, s): s for s in missing}
            for fut in as_completed(futures):
                sym = futures[fut]
                q = fut.result()
                if q:
                    out[sym] = q

    missing = [s for s in eligible if s not in out]
    if missing:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(_stooq, s): s for s in missing}
            for fut in as_completed(futures):
                sym = futures[fut]
                q = fut.result()
                if q:
                    out[sym] = q

    n_v7 = sum(1 for v in out.values() if v.get("source") == "yahoo-v7")
    n_v8 = sum(1 for v in out.values() if v.get("source") == "yahoo-v8")
    n_st = sum(1 for v in out.values() if v.get("source") == "stooq")
    LOG.info("prices: %d/%d in %.2fs (v7=%d, v8=%d, stooq=%d)",
             len(out), len(eligible), time.time() - started, n_v7, n_v8, n_st)
    return out

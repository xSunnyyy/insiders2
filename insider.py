"""Insider buying via openinsider.com.

openinsider.com publishes Form 4 (insider trade) filings in plain HTML
tables. We scrape the "latest insider purchases >= $25k" page, parse the
row data with regex (no BeautifulSoup dep), and return structured rows.

Cached for 1h in memory; HTML scrapes are fragile so failures degrade to
an empty list instead of raising.
"""

from __future__ import annotations

import logging
import re
import time
from html import unescape

import requests

LOG = logging.getLogger(__name__)

URL = "http://openinsider.com/insider-purchases-25k"
CLUSTER_URL = "http://openinsider.com/latest-cluster-buys"
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

CACHE_TTL_SEC = 60 * 60
_cache: dict = {"purchases": None, "clusters": None, "fetched_at": 0.0}

# openinsider rows are <tr>...<td>cells</td></tr> with consistent columns.
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")


def _strip(html: str) -> str:
    return unescape(TAG_RE.sub("", html or "")).strip()


def _to_int(s: str) -> int:
    s = (s or "").replace(",", "").replace("+", "").strip()
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def _to_float(s: str) -> float:
    s = (s or "").replace(",", "").replace("$", "").replace("+", "").strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _parse(html: str) -> list[dict]:
    """Best-effort parse of openinsider.com's transaction tables.

    Their table columns vary slightly by view, but the shape we care
    about is consistent: filing date, transaction date, ticker, company,
    insider name, title, transaction type, last price, qty, value, etc.
    """
    rows: list[dict] = []
    for tr in ROW_RE.findall(html):
        cells = [_strip(c) for c in CELL_RE.findall(tr)]
        if len(cells) < 12:
            continue
        ticker = cells[3].upper()
        # First-cell is sometimes an arrow icon / X mark; ignore.
        if not ticker or not re.match(r"^[A-Z]{1,6}(?:\.[A-Z])?$", ticker):
            continue
        txn_type = cells[7]
        if "P" not in txn_type:
            # Only keep purchases (P - Purchase). Skip S/A/D/etc.
            continue
        rows.append({
            "filing_date": cells[1],
            "trade_date": cells[2],
            "ticker": ticker,
            "company": cells[4],
            "insider_name": cells[5],
            "title": cells[6],
            "txn_type": txn_type,
            "price": _to_float(cells[8]),
            "qty": _to_int(cells[9]),
            "owned": _to_int(cells[10]) if len(cells) > 10 else 0,
            "value": _to_float(cells[12]) if len(cells) > 12 else 0.0,
        })
    return rows


def _fetch(url: str) -> str:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=12)
        if r.status_code != 200:
            LOG.warning("openinsider %s -> %s", url, r.status_code)
            return ""
        return r.text
    except requests.RequestException as e:
        LOG.warning("openinsider request failed: %s", e)
        return ""


def _refresh() -> None:
    p_html = _fetch(URL)
    c_html = _fetch(CLUSTER_URL)
    _cache["purchases"] = _parse(p_html) if p_html else []
    _cache["clusters"] = _parse(c_html) if c_html else []
    _cache["fetched_at"] = time.time()


def purchases(limit: int = 50) -> list[dict]:
    if _cache["purchases"] is None \
            or (time.time() - _cache["fetched_at"]) > CACHE_TTL_SEC:
        _refresh()
    return (_cache["purchases"] or [])[:limit]


def cluster_buys(limit: int = 30) -> list[dict]:
    if _cache["clusters"] is None \
            or (time.time() - _cache["fetched_at"]) > CACHE_TTL_SEC:
        _refresh()
    return (_cache["clusters"] or [])[:limit]


def by_ticker(symbols: list[str], days: int = 30) -> dict[str, list[dict]]:
    """Group recent purchases by ticker for the given symbols."""
    syms = {s.upper() for s in symbols}
    out: dict[str, list[dict]] = {s: [] for s in syms}
    cutoff_ts = time.time() - days * 86400
    for row in purchases(limit=500):
        if row["ticker"] in syms:
            # Trade dates are like "2025-05-10". Parse loosely.
            try:
                t = time.mktime(time.strptime(row["trade_date"][:10], "%Y-%m-%d"))
                if t < cutoff_ts:
                    continue
            except (ValueError, IndexError):
                pass
            out[row["ticker"]].append(row)
    return out

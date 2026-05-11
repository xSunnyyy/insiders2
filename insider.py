"""Insider buying via openinsider.com.

openinsider.com publishes Form 4 (insider trade) filings in HTML tables
with class="tinytable". We scrape the latest insider purchases page,
parse the trade table directly, and return structured rows.

Failure modes are surfaced via `last_error()` so the dashboard can show
*why* a result was empty instead of a silent zero.
"""

from __future__ import annotations

import logging
import re
import time
from html import unescape

import requests

LOG = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

# openinsider has both http and https variants depending on the page.
# We try http first (their canonical), https second.
SOURCES = {
    "purchases": [
        "http://openinsider.com/insider-purchases-25k",
        "http://openinsider.com/top-insider-purchases-of-the-week",
    ],
    "clusters": [
        "http://openinsider.com/latest-cluster-buys",
    ],
}

CACHE_TTL_SEC = 60 * 60
_cache: dict = {"purchases": None, "clusters": None,
                "fetched_at": 0.0}
_last_error: str | None = None


def last_error() -> str | None:
    return _last_error


# Targeted regexes: pull only rows inside <table class="tinytable">.
TABLE_RE = re.compile(
    r'<table[^>]*class="[^"]*tinytable[^"]*"[^>]*>(.*?)</table>',
    re.DOTALL | re.IGNORECASE,
)
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.DOTALL | re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")

TICKER_RE = re.compile(r"^[A-Z]{1,6}(?:\.[A-Z])?$")


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


TICKER_LINK_RE = re.compile(r'<a\s+href="/([A-Z]{1,6}(?:\.[A-Z])?)"', re.IGNORECASE)


def _parse(html: str) -> list[dict]:
    """Extract Form-4 purchase rows from the tinytable.

    openinsider has multiple page layouts (purchases-25k, cluster-buys,
    top-of-the-week) with slightly different column counts (13-17) and
    different field positions. We anchor on:
      - cell[1] = filing date (with time on cluster pages)
      - cell[2] = trade date
      - cell[3] = ticker  (extracted from the <a href="/SYM"> link, since
                           the cell text is polluted by JS tooltips)
      - cell[4] = company name
      - cell[7] = transaction type   ('P - Purchase', ...)
      - cell[8] = price              (may have $ prefix)
      - cell[9] = qty                (may have +/- prefix)
      - cell[10] = owned
      - cell[12] = value             (may have +/$ prefix)

    Insider name / title are NOT in cluster pages (cells[5] = industry,
    cells[6] = filer count there). We surface what we have.
    """
    rows: list[dict] = []
    tables = TABLE_RE.findall(html or "")
    if not tables:
        return rows
    # The trade table is the largest tinytable.
    tables.sort(key=len, reverse=True)
    target = tables[0]

    for tr in ROW_RE.findall(target):
        if "<th" in tr.lower():
            continue
        raw_cells = CELL_RE.findall(tr)
        if len(raw_cells) < 12:
            continue
        # Pull ticker from the <a href="/SYM"> link in cell[3], not from
        # the cell text (which has JS tooltip junk on cluster pages).
        tlink = TICKER_LINK_RE.search(raw_cells[3])
        if not tlink:
            continue
        ticker = tlink.group(1).upper()
        cells = [_strip(c) for c in raw_cells]
        txn_type = cells[7]
        if "P" not in txn_type:
            continue

        # Distinguish page layouts: the cluster-buys page puts industry in
        # cell[5] and filer count in cell[6]. The purchases-25k page puts
        # insider name in cell[5] and title in cell[6]. Heuristic: cell[6]
        # is a small integer on cluster pages.
        is_cluster_layout = cells[6].isdigit() and len(cells[6]) <= 2

        rows.append({
            "filing_date": cells[1],
            "trade_date": cells[2],
            "ticker": ticker,
            "company": cells[4],
            "insider_name": "" if is_cluster_layout else cells[5],
            "title": (f"{cells[6]} insiders" if is_cluster_layout else cells[6]),
            "industry": cells[5] if is_cluster_layout else "",
            "txn_type": txn_type,
            "price": _to_float(cells[8]),
            "qty": _to_int(cells[9]),
            "owned": _to_int(cells[10]),
            "value": _to_float(cells[12]) if len(cells) > 12 else 0.0,
        })
    return rows


def _fetch(url: str) -> tuple[int, str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=12,
                         allow_redirects=True)
        return r.status_code, r.text
    except requests.RequestException as e:
        LOG.warning("openinsider fetch failed: %s", e)
        return 0, f"transport: {e}"


def _refresh() -> None:
    global _last_error
    _last_error = None
    diag: list[str] = []

    def fetch_first_working(urls: list[str]) -> list[dict]:
        for u in urls:
            status, body = _fetch(u)
            n_bytes = len(body) if isinstance(body, str) else 0
            if status != 200:
                diag.append(f"{u} -> {status}")
                continue
            parsed = _parse(body)
            diag.append(f"{u} -> {status}, {n_bytes}b, {len(parsed)} rows")
            if parsed:
                return parsed
        return []

    purchases = fetch_first_working(SOURCES["purchases"])
    clusters = fetch_first_working(SOURCES["clusters"])

    _cache["purchases"] = purchases
    _cache["clusters"] = clusters
    # Cache empty results only briefly so we keep retrying.
    _cache["fetched_at"] = (
        time.time() if (purchases or clusters)
        else time.time() - (CACHE_TTL_SEC - 90)   # retry in ~90s
    )
    if not purchases and not clusters:
        _last_error = "; ".join(diag) or "no source returned data"
        LOG.warning("insider: %s", _last_error)


def _ensure_cache() -> None:
    if _cache["purchases"] is None \
            or (time.time() - _cache["fetched_at"]) > CACHE_TTL_SEC:
        _refresh()
        # If openinsider failed across the board, fall back to Yahoo's
        # insiderTransactions module across our curated universe.
        if not _cache["purchases"] and not _cache["clusters"]:
            _refresh_from_yahoo()


# ---------------------------------------------------------------------------
# Yahoo fallback: works from datacenter IPs (uses our shared crumb).
# ---------------------------------------------------------------------------

import yahoo_auth  # noqa: E402
from concurrent.futures import ThreadPoolExecutor, as_completed  # noqa: E402

YAHOO_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
# Curated universe of widely-traded tickers (kept small so it fits the
# Vercel 60s budget even on a cold start).
UNIVERSE: list[str] = []


def _load_universe() -> list[str]:
    global UNIVERSE
    if UNIVERSE:
        return UNIVERSE
    try:
        from tickers import KNOWN_TICKERS
        UNIVERSE = sorted(KNOWN_TICKERS)[:120]
    except Exception:
        UNIVERSE = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL",
                    "META", "JPM", "BAC", "WFC", "XOM", "CVX"]
    return UNIVERSE


# Yahoo's insiderTransactions returns objects like:
#   {filerName, filerRelation, transactionText, moneyText, shares: {raw, fmt},
#    value: {raw, fmt}, startDate: {raw, fmt}, ownership}
# transactionText examples: "Purchase at price 123.45 per share",
#                           "Sale at price ...", "Award", "Stock Gift", etc.
PURCHASE_TEXT_RE = re.compile(r"^\s*purchase", re.IGNORECASE)


def _yahoo_for(symbol: str) -> list[dict]:
    sym = symbol.replace(".", "-")
    try:
        session, crumb = yahoo_auth.get()
        params = {"modules": "insiderTransactions"}
        if crumb:
            params["crumb"] = crumb
        r = session.get(YAHOO_URL.format(symbol=sym), params=params, timeout=8)
        if r.status_code != 200:
            return []
        result = ((r.json() or {}).get("quoteSummary") or {}).get("result") or []
        if not result:
            return []
        trans = (result[0].get("insiderTransactions") or {}).get("transactions") or []
        out: list[dict] = []
        for t in trans:
            text = (t.get("transactionText") or "")
            if not PURCHASE_TEXT_RE.match(text):
                continue
            shares = (t.get("shares") or {}).get("raw") or 0
            value = (t.get("value") or {}).get("raw") or 0
            start = (t.get("startDate") or {}).get("fmt") or ""
            # Derive price from "Purchase at price 123.45 per share" when present
            pm = re.search(r"([\d.]+)\s*per share", text)
            price = float(pm.group(1)) if pm else (
                (value / shares) if shares else 0.0
            )
            out.append({
                "filing_date": start,
                "trade_date": start,
                "ticker": symbol,
                "company": "",
                "insider_name": t.get("filerName") or "",
                "title": t.get("filerRelation") or "",
                "txn_type": "P - Purchase",
                "price": round(float(price), 2),
                "qty": int(shares or 0),
                "owned": int((t.get("ownership") or 0)),
                "value": float(value or 0),
            })
        return out
    except (requests.RequestException, ValueError, KeyError, TypeError) as e:
        LOG.debug("yahoo insider fetch failed for %s: %s", symbol, e)
        return []


def _refresh_from_yahoo() -> None:
    global _last_error
    started = time.time()
    universe = _load_universe()
    all_trades: list[dict] = []
    fetched = with_data = 0
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(_yahoo_for, s): s for s in universe}
        for fut in as_completed(futures):
            fetched += 1
            rows = fut.result() or []
            if rows:
                with_data += 1
                all_trades.extend(rows)

    # Sort newest first
    all_trades.sort(key=lambda r: r.get("trade_date", ""), reverse=True)

    # Cluster buys: tickers with >= 2 distinct insiders in last 30 days
    by_t: dict[str, set] = {}
    for r in all_trades:
        by_t.setdefault(r["ticker"], set()).add(r["insider_name"])
    cluster_syms = {t for t, names in by_t.items() if len(names) >= 2}
    clusters = [r for r in all_trades if r["ticker"] in cluster_syms]

    _cache["purchases"] = all_trades
    _cache["clusters"] = clusters
    if all_trades:
        _cache["fetched_at"] = time.time()
        tag = (f"yahoo fallback: scanned {fetched}/{len(universe)} tickers in "
               f"{time.time()-started:.1f}s, {with_data} had purchase trades, "
               f"{len(all_trades)} total")
        _last_error = ((_last_error or "") + f" | {tag}").lstrip(" |")
        LOG.info("insider: %s", tag)
    else:
        _last_error = ((_last_error or "")
                       + f" | yahoo fallback returned 0 across "
                       f"{fetched} tickers in {time.time()-started:.1f}s")


def purchases(limit: int = 50) -> list[dict]:
    _ensure_cache()
    return (_cache["purchases"] or [])[:limit]


def cluster_buys(limit: int = 30) -> list[dict]:
    _ensure_cache()
    return (_cache["clusters"] or [])[:limit]


def by_ticker(symbols: list[str], days: int = 30) -> dict[str, list[dict]]:
    """Group recent purchases by ticker for the given symbols."""
    syms = {s.upper() for s in symbols}
    out: dict[str, list[dict]] = {s: [] for s in syms}
    cutoff_ts = time.time() - days * 86400
    for row in purchases(limit=500):
        if row["ticker"] in syms:
            try:
                t = time.mktime(time.strptime(row["trade_date"][:10], "%Y-%m-%d"))
                if t < cutoff_ts:
                    continue
            except (ValueError, IndexError):
                pass
            out[row["ticker"]].append(row)
    return out

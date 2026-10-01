"""AltIndex-style trending lists built entirely from free, keyless sources.

Data sources
------------
- ApeWisdom (apewisdom.io/api)   mention counts + counts 24h ago for every
                                 major finance subreddit and 4chan /biz/.
- Stocktwits trending symbols    rank, trending score, company name, logo.
- Stocktwits symbol streams      poster-tagged Bullish/Bearish messages,
                                 used for per-ticker sentiment.
- Tradestie                      WallStreetBets sentiment for the top ~50.
- 4chan /biz/ catalog            thread text scored with our VADER lexicon.
- Yahoo v8 chart (3 months)      price, day change, 1-month return, 50-day MA.
  (falls back to prices.fetch_quotes -> Finnhub on cloud hosts)
- insider.py cache               recent Form-4 open-market purchases.

Every list row has the same shape so one table renderer handles them all:
    symbol, name, logo, mentions, mentions_prev, mention_change_pct,
    sentiment (bullish|bearish|neutral), sentiment_score, price,
    change_pct, ret_1m, score (0-100), score_parts
"""

from __future__ import annotations

import html
import logging
import math
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import requests

import insider as insider_mod
import prices
import sentiment as sent_mod
from tickers import extract_tickers

LOG = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
APE_URL = "https://apewisdom.io/api/v1.0/filter/{filter}/page/{page}"
ST_TRENDING_URL = "https://api.stocktwits.com/api/2/trending/symbols.json"
ST_STREAM_URL = "https://api.stocktwits.com/api/2/streams/symbol/{sym}.json"
TRADESTIE_URL = "https://tradestie.com/api/v1/apps/reddit"
BIZ_CATALOG_URL = "https://a.4cdn.org/biz/catalog.json"
CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"

LIST_TTL = 10 * 60          # per-list payload
SOURCE_TTL = 10 * 60        # raw ApeWisdom / Stocktwits / 4chan responses
SENT_TTL = 45 * 60          # per-symbol Stocktwits sentiment (rate-limited API)
PRICE_TTL = 5 * 60          # per-symbol price history
ROWS_PER_LIST = 25


# ---------------------------------------------------------------------------
# List catalogue. `kind` decides how rows are produced.
# ---------------------------------------------------------------------------

LISTS: dict[str, dict] = {
    "wallstreetbets": {
        "title": "Trending Stocks on WallStreetBets", "group": "social",
        "short": "WallStreetBets", "icon": "🦍",
        "desc": "Stocks getting the most r/wallstreetbets attention right now.",
        "kind": "ape", "filters": ["wallstreetbets"],
    },
    "reddit": {
        "title": "Most Mentioned Stocks on Reddit", "group": "social",
        "short": "Reddit", "icon": "👽",
        "desc": "Most-mentioned tickers across r/wallstreetbets, r/stocks, "
                "r/investing, r/options and the rest of finance Reddit.",
        "kind": "ape", "filters": ["all-stocks"],
    },
    "reddit-buy": {
        "title": "Trending Reddit Stocks to Buy", "group": "social",
        "short": "Reddit to Buy", "icon": "🎯",
        "desc": "The same Reddit chatter, filtered to names our score rates "
                "a buy and ranked by score instead of mention volume.",
        "kind": "derived", "base": "reddit", "min_score": 60,
        "sort": "score",
    },
    "stocktwits": {
        "title": "Trending Stocks on StockTwits", "group": "social",
        "short": "StockTwits", "icon": "💬",
        "desc": "StockTwits' own trending ranking, with poster-tagged "
                "bull/bear sentiment.",
        "kind": "stocktwits", "metric_label": "Trend score",
    },
    "4chan": {
        "title": "Trending Stocks on 4chan /biz/", "group": "social",
        "short": "4chan", "icon": "🍀",
        "desc": "Tickers the anons on /biz/ won't shut up about.",
        "kind": "ape", "filters": ["4chan"], "native_sentiment": "biz",
    },
    "stocks": {
        "title": "Trending on r/stocks", "group": "communities",
        "short": "r/stocks", "icon": "📊",
        "desc": "The more buttoned-up side of Reddit stock talk.",
        "kind": "ape", "filters": ["stocks"],
    },
    "investing": {
        "title": "Trending on r/investing", "group": "communities",
        "short": "r/investing", "icon": "🏦",
        "desc": "Long-term investor chatter -- ETFs and blue chips.",
        "kind": "ape", "filters": ["investing"],
    },
    "options": {
        "title": "Trending on r/options", "group": "communities",
        "short": "r/options", "icon": "🎲",
        "desc": "What options traders are positioning around.",
        "kind": "ape", "filters": ["options"],
    },
    "daytrading": {
        "title": "Trending on r/Daytrading", "group": "communities",
        "short": "r/Daytrading", "icon": "⚡",
        "desc": "Intraday momentum names.",
        "kind": "ape", "filters": ["Daytrading"],
    },
    "penny-stocks": {
        "title": "Trending Penny Stocks", "group": "communities",
        "short": "Penny stocks", "icon": "🪙",
        "desc": "Most-mentioned tickers on r/pennystocks. High risk.",
        "kind": "ape", "filters": ["pennystocks"],
    },
    "short-squeeze": {
        "title": "Short Squeeze Candidates", "group": "themes",
        "short": "Short squeeze", "icon": "🩳",
        "desc": "Tickers being pitched on r/Shortsqueeze and r/SqueezePlays.",
        "kind": "ape", "filters": ["Shortsqueeze", "SqueezePlays"],
    },
    "meme-stocks": {
        "title": "Meme Stocks", "group": "themes",
        "short": "Meme stocks", "icon": "🚀",
        "desc": "WSB-family names with the biggest 24h jump in chatter.",
        "kind": "ape", "sort": "momentum", "min_mentions": 15,
        "filters": ["wallstreetbets", "WallStreetbetsELITE",
                    "Wallstreetbetsnew"],
    },
    "rising": {
        "title": "Fastest Rising on Social", "group": "themes",
        "short": "Fastest rising", "icon": "📈",
        "desc": "Biggest 24h mention growth across all of Reddit "
                "(min. 20 mentions).",
        "kind": "derived", "base": "reddit", "sort": "momentum",
        "min_mentions": 20,
    },
    "insider-buying": {
        "title": "Stocks Insiders Are Buying", "group": "insiders",
        "short": "Insider buying", "icon": "🕴️",
        "desc": "Open-market Form 4 purchases by officers and directors, "
                "grouped by ticker.", "kind": "insider",
        "metric_label": "Insider $",
    },
}

GROUPS = [
    ("social", "Social media trending",
     "Stocks getting the most attention across major social platforms."),
    ("communities", "Investing communities",
     "Subreddit-by-subreddit breakdown."),
    ("themes", "Themes", "Squeezes, memes and momentum."),
    ("insiders", "Insiders & catalysts",
     "Follow the people who actually know the company."),
]


# ---------------------------------------------------------------------------
# Small TTL cache
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, object]] = {}
_cache_lock = threading.Lock()


def _cached(key: str, ttl: float, fn):
    with _cache_lock:
        hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    val = fn()
    if val is not None:
        with _cache_lock:
            _cache[key] = (time.time(), val)
        return val
    return hit[1] if hit else None   # serve stale on failure


def _get_json(url: str, timeout: float = 10, **kw):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, **kw)
        if r.status_code != 200:
            LOG.warning("%s -> %s", url, r.status_code)
            return None
        return r.json()
    except (requests.RequestException, ValueError) as e:
        LOG.warning("%s failed: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# Raw sources
# ---------------------------------------------------------------------------

def _ape(filt: str, pages: int = 1) -> list[dict]:
    def load():
        out: list[dict] = []
        for p in range(1, pages + 1):
            d = _get_json(APE_URL.format(filter=filt, page=p))
            if not d:
                break
            out.extend(d.get("results") or [])
            if p >= int(d.get("pages") or 1):
                break
        return out or None
    return _cached(f"ape:{filt}:{pages}", SOURCE_TTL, load) or []


def _stocktwits_trending() -> list[dict]:
    def load():
        d = _get_json(ST_TRENDING_URL)
        return (d or {}).get("symbols") or None
    return _cached("st:trending", SOURCE_TTL, load) or []


def _tradestie() -> dict[str, float]:
    """WSB sentiment for ~50 tickers. Raw scores are small (~±0.2), so we
    stretch them into [-1, 1]."""
    def load():
        d = _get_json(TRADESTIE_URL)
        if not isinstance(d, list):
            return None
        return {row["ticker"].upper(): max(-1.0, min(1.0, float(row.get("sentiment_score") or 0) * 5))
                for row in d if row.get("ticker")}
    return _cached("tradestie", SOURCE_TTL, load) or {}


_TAG_RE = re.compile(r"<[^>]+>")


def _biz_sentiment() -> dict[str, float]:
    """Score every /biz/ catalog thread with our lexicon and average per
    ticker. One request covers the whole board."""
    def load():
        d = _get_json(BIZ_CATALOG_URL, timeout=15)
        if not isinstance(d, list):
            return None
        acc: dict[str, list[float]] = defaultdict(list)
        for page in d:
            for th in page.get("threads") or []:
                texts = [th.get("sub", ""), th.get("com", "")]
                texts += [r.get("com", "") for r in th.get("last_replies") or []]
                for raw in texts:
                    text = html.unescape(_TAG_RE.sub(" ", raw or ""))
                    syms = extract_tickers(text)
                    if not syms:
                        continue
                    s = sent_mod.score(text)
                    for sym in syms:
                        acc[sym].append(s)
        return {k: sum(v) / len(v) for k, v in acc.items() if v}
    return _cached("biz", SOURCE_TTL, load) or {}


def _stocktwits_sentiment(sym: str) -> float | None:
    """Bull/bear ratio from the latest ~30 Stocktwits messages."""
    def load():
        d = _get_json(ST_STREAM_URL.format(sym=sym), timeout=8)
        if not d:
            return None
        scores: list[float] = []
        for m in d.get("messages") or []:
            tag = ((m.get("entities") or {}).get("sentiment") or {}).get("basic")
            if tag == "Bullish":
                scores.append(1.0)
            elif tag == "Bearish":
                scores.append(-1.0)
            else:
                scores.append(sent_mod.score(m.get("body") or ""))
        return {"s": sum(scores) / len(scores) if scores else 0.0,
                "n": len(scores)}
    v = _cached(f"stsent:{sym}", SENT_TTL, load)
    return v["s"] if v and v["n"] else None


NASDAQ_URL = "https://api.nasdaq.com/api/quote/{sym}/{what}"
NASDAQ_HEADERS = {"User-Agent": UA, "Accept": "application/json",
                  "Origin": "https://www.nasdaq.com",
                  "Referer": "https://www.nasdaq.com/"}


def _money(s) -> float | None:
    try:
        return float(str(s).replace("$", "").replace(",", "").replace("%", "")
                     .replace("+", "").strip())
    except (TypeError, ValueError):
        return None


def _nasdaq(sym: str, what: str, **params) -> dict | None:
    """Nasdaq's public quote API. Tries stocks first, then ETFs."""
    for cls in ("stocks", "etf"):
        try:
            r = requests.get(NASDAQ_URL.format(sym=sym.replace(".", "^"), what=what),
                             params={"assetclass": cls, **params},
                             headers=NASDAQ_HEADERS, timeout=8)
            data = (r.json() or {}).get("data") if r.status_code == 200 else None
        except (requests.RequestException, ValueError):
            data = None
        if data:
            return data
    return None


def _closes_from_nasdaq(sym: str) -> list[float]:
    today = time.strftime("%Y-%m-%d")
    since = time.strftime("%Y-%m-%d", time.localtime(time.time() - 110 * 86400))
    data = None
    for _ in range(2):      # the vendor behind this endpoint flakes often
        data = _nasdaq(sym, "historical", fromdate=since, todate=today, limit=80)
        if data:
            break
    rows = (((data or {}).get("tradesTable") or {}).get("rows")) or []
    closes = [_money(r.get("close")) for r in reversed(rows)]   # oldest first
    return [c for c in closes if c]


def _closes_from_yahoo(sym: str) -> tuple[list[float], dict]:
    try:
        r = requests.get(CHART_URL.format(sym=sym.replace(".", "-")),
                         params={"interval": "1d", "range": "3mo"},
                         headers={"User-Agent": UA}, timeout=8)
        if r.status_code != 200:
            return [], {}
        res = ((r.json() or {}).get("chart") or {}).get("result") or []
        if not res:
            return [], {}
        quote = ((res[0].get("indicators") or {}).get("quote") or [{}])[0]
        return ([c for c in quote.get("close") or [] if c is not None],
                res[0].get("meta") or {})
    except (requests.RequestException, ValueError, TypeError):
        return [], {}


def _price_one(sym: str) -> dict | None:
    """Price, day change, 1-month return, 50-day-MA position, sparkline.
    Nasdaq first (works from most hosts, no key), Yahoo chart second."""
    def load():
        info = _nasdaq(sym, "info") or {}
        primary = info.get("primaryData") or {}
        price = _money(primary.get("lastSalePrice"))
        change_pct = _money(primary.get("percentageChange"))
        name = info.get("companyName")
        closes = _closes_from_nasdaq(sym)
        if len(closes) < 2:
            closes, meta = _closes_from_yahoo(sym)
            name = name or meta.get("longName") or meta.get("shortName")
            if price is None and meta.get("regularMarketPrice") is not None:
                price = float(meta["regularMarketPrice"])
        if price is None and closes:
            price = closes[-1]
        if price is None:
            return None
        if change_pct is None and len(closes) >= 2 and closes[-2]:
            change_pct = (price - closes[-2]) / closes[-2] * 100
        ret_1m = above = None
        if len(closes) >= 10:
            month_ago = closes[-22] if len(closes) >= 22 else closes[0]
            ret_1m = round((price - month_ago) / month_ago * 100, 2)
            ma50 = sum(closes[-50:]) / len(closes[-50:])
            above = price >= ma50
        return {
            "price": round(price, 2),
            "change_pct": round(change_pct, 2) if change_pct is not None else None,
            "ret_1m": ret_1m, "above_ma50": above,
            "spark": [round(c, 2) for c in closes[-30:]],
            "name": _clean_name(name),
        }
    return _cached(f"px:{sym}", PRICE_TTL, load)


_NAME_SUFFIX = re.compile(r"\s+(Class [A-C] )?(Common Stock|Ordinary Shares|"
                          r"American Depositary Shares|Common Shares)\b.*$", re.I)


def _clean_name(name: str | None) -> str | None:
    if not name:
        return None
    return _NAME_SUFFIX.sub("", name).strip()


def _prices(symbols: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        for sym, res in zip(symbols, ex.map(_price_one, symbols)):
            if res:
                out[sym] = res
    missing = [s for s in symbols if s not in out]
    if missing:
        # Yahoo is often blocked from cloud IPs; prices.fetch_quotes knows
        # about Stooq and Finnhub.
        for sym, q in (prices.fetch_quotes(missing) or {}).items():
            if q.get("price") is not None:
                out[sym] = {"price": q["price"], "change_pct": q.get("change_pct"),
                            "ret_1m": None, "above_ma50": None, "spark": []}
    return out


def _sentiments(symbols: list[str], native: dict[str, float] | None) -> dict[str, float]:
    wsb = _tradestie()
    with ThreadPoolExecutor(max_workers=8) as ex:
        st = dict(zip(symbols, ex.map(_stocktwits_sentiment, symbols)))
    out: dict[str, float] = {}
    for sym in symbols:
        parts = [v for v in (st.get(sym), wsb.get(sym),
                             (native or {}).get(sym)) if v is not None]
        if parts:
            out[sym] = sum(parts) / len(parts)
    return out


def _insider_index() -> dict[str, dict]:
    """{ticker: {"value": $, "count": n}} from whatever the insider module has
    cached. Never blocks; kicks a background warm-up if the cache is cold."""
    cache = insider_mod._cache.get("purchases")
    if cache is None:
        _warm_insider_async()
        return {}
    idx: dict[str, dict] = {}
    cutoff = time.time() - 30 * 86400
    for row in cache:
        try:
            t = time.mktime(time.strptime((row.get("trade_date") or "")[:10], "%Y-%m-%d"))
            if t < cutoff:
                continue
        except ValueError:
            pass
        e = idx.setdefault(row["ticker"], {"value": 0.0, "count": 0,
                                            "company": row.get("company") or ""})
        e["value"] += float(row.get("value") or 0)
        e["count"] += 1
    return idx


_insider_warming = threading.Event()


def _warm_insider_async() -> None:
    if _insider_warming.is_set():
        return
    _insider_warming.set()

    def run():
        try:
            insider_mod.purchases(limit=1)
        except Exception as e:  # noqa: BLE001
            LOG.warning("insider warm-up failed: %s", e)
        finally:
            _insider_warming.clear()
    threading.Thread(target=run, daemon=True).start()


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def compute_score(row: dict, insiders: dict[str, dict]) -> tuple[int | None, dict]:
    """Blend of five signals, each normalised to 0..1. Missing signals are
    dropped and the remaining weights re-normalised. This is a transparent
    heuristic, not a forecast."""
    parts: dict[str, tuple[float, float]] = {}
    s = row.get("sentiment_score")
    if s is not None:
        parts["sentiment"] = (0.35, _clamp01((s + 1) / 2))
    m, mp = row.get("mentions"), row.get("mentions_prev")
    if m is not None and mp is not None and row.get("metric") == "mentions":
        ratio = math.log2((m + 1) / (mp + 1))      # +1 = doubled
        parts["buzz"] = (0.20, _clamp01((ratio + 2) / 4))
    if row.get("change_pct") is not None:
        parts["price_day"] = (0.10, _clamp01((row["change_pct"] + 8) / 16))
    if row.get("ret_1m") is not None:
        trend = _clamp01((row["ret_1m"] + 25) / 50)
        if row.get("above_ma50") is not None:
            trend = 0.7 * trend + 0.3 * (1.0 if row["above_ma50"] else 0.0)
        parts["trend"] = (0.20, trend)
    ins = insiders.get(row["symbol"])
    if insiders:
        parts["insider"] = (0.15, _clamp01(0.5 + (math.log10(ins["value"] + 1) / 14
                                                    if ins else 0)))
    if not parts or ("sentiment" not in parts and "trend" not in parts):
        return None, {}
    wsum = sum(w for w, _ in parts.values())
    score = sum(w * v for w, v in parts.values()) / wsum
    return round(score * 100), {k: round(v * 100) for k, (_, v) in parts.items()}


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

def _logo(sym: str) -> str:
    return f"https://financialmodelingprep.com/image-stock/{sym}.png"


def _ape_rows(cfg: dict) -> list[dict]:
    merged: dict[str, dict] = {}
    for f in cfg["filters"]:
        for r in _ape(f, pages=2):
            sym = (r.get("ticker") or "").upper()
            if not sym or "." in sym:
                continue
            e = merged.setdefault(sym, {
                "symbol": sym, "name": html.unescape(r.get("name") or sym),
                "mentions": 0, "mentions_prev": 0, "upvotes": 0})
            e["mentions"] += int(r.get("mentions") or 0)
            e["mentions_prev"] += int(r.get("mentions_24h_ago") or 0)
            e["upvotes"] += int(r.get("upvotes") or 0)
    rows = list(merged.values())
    for r in rows:
        r["metric"] = "mentions"
    min_m = cfg.get("min_mentions", 0)
    rows = [r for r in rows if r["mentions"] >= min_m]
    if cfg.get("sort") == "momentum":
        rows.sort(key=lambda r: (r["mentions"] + 1) / (r["mentions_prev"] + 5),
                  reverse=True)
    else:
        rows.sort(key=lambda r: r["mentions"], reverse=True)
    return rows


def _stocktwits_rows(cfg: dict) -> list[dict]:
    rows = []
    for s in _stocktwits_trending():
        if (s.get("instrument_class") or "Stock") not in ("Stock", "ETF"):
            continue
        sym = (s.get("symbol") or "").upper()
        if not sym or "." in sym:
            continue
        rows.append({
            "symbol": sym, "name": s.get("title") or sym,
            "logo": s.get("logo_url"),
            "mentions": round(float(s.get("trending_score") or 0), 1),
            "mentions_prev": None, "metric": "trend_score",
            "summary": (s.get("trends") or {}).get("summary"),
        })
    return rows


def _insider_rows(cfg: dict) -> list[dict]:
    try:
        insider_mod.purchases(limit=1)    # blocking warm-up on this page only
    except Exception as e:  # noqa: BLE001
        LOG.warning("insider fetch failed: %s", e)
    idx = _insider_index()
    rows = [{"symbol": sym, "name": v["company"] or sym,
             "mentions": round(v["value"]), "mentions_prev": None,
             "insider_count": v["count"], "metric": "insider_value"}
            for sym, v in idx.items()]
    rows.sort(key=lambda r: r["mentions"], reverse=True)
    return rows


def _enrich(rows: list[dict], native: dict[str, float] | None = None) -> list[dict]:
    syms = [r["symbol"] for r in rows]
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_px = ex.submit(_prices, syms)
        f_st = ex.submit(_sentiments, syms, native)
        px, st = f_px.result(), f_st.result()
    insiders = _insider_index()
    for r in rows:
        sym = r["symbol"]
        p = px.get(sym) or {}
        r.update({k: p.get(k) for k in ("price", "change_pct", "ret_1m",
                                        "above_ma50", "spark")})
        if (not r.get("name") or r["name"] == sym) and p.get("name"):
            r["name"] = p["name"]
        r.setdefault("logo", None)
        r["logo"] = r["logo"] or _logo(sym)
        s = st.get(sym)
        r["sentiment_score"] = round(s, 3) if s is not None else None
        r["sentiment"] = sent_mod.label(s) if s is not None else "neutral"
        mp = r.get("mentions_prev")
        r["mention_change_pct"] = (
            round((r["mentions"] - mp) / mp * 100, 1) if mp else None)
        r["insider_buys"] = (insiders.get(sym) or {}).get("count", 0)
        r["score"], r["score_parts"] = compute_score(r, insiders)
    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_list_cache: dict[str, dict] = {}
_list_lock = threading.Lock()
_in_flight: set[str] = set()
_bg_pool = ThreadPoolExecutor(max_workers=2)


def _build(slug: str) -> dict:
    cfg = LISTS[slug]
    kind = cfg["kind"]
    started = time.time()
    if kind == "derived":
        base = get_list(cfg["base"], allow_stale=True)
        rows = [dict(r) for r in base["rows"]]
        if cfg.get("min_score"):
            rows = [r for r in rows if (r.get("score") or 0) >= cfg["min_score"]]
            rows.sort(key=lambda r: r.get("score") or 0, reverse=True)
        if cfg.get("min_mentions"):
            rows = [r for r in rows if r["mentions"] >= cfg["min_mentions"]]
        if cfg.get("sort") == "momentum":
            # Base list only carries the top N, so pull the full feed instead.
            raw = _ape_rows({"filters": LISTS[cfg["base"]]["filters"],
                             "sort": "momentum",
                             "min_mentions": cfg.get("min_mentions", 0)})
            rows = _enrich(raw[:ROWS_PER_LIST])
    else:
        native = _biz_sentiment() if cfg.get("native_sentiment") == "biz" else None
        if kind == "ape":
            raw = _ape_rows(cfg)
        elif kind == "stocktwits":
            raw = _stocktwits_rows(cfg)
        elif kind == "insider":
            raw = _insider_rows(cfg)
        else:
            raw = []
        rows = _enrich(raw[:ROWS_PER_LIST], native)
    return {
        "slug": slug, "title": cfg["title"], "desc": cfg["desc"],
        "metric_label": cfg.get("metric_label", "Mentions"),
        "rows": rows, "generated_at": int(time.time()),
        "duration_sec": round(time.time() - started, 2),
    }


def _refresh(slug: str) -> dict:
    data = _build(slug)
    with _list_lock:
        _list_cache[slug] = data
        _in_flight.discard(slug)
    return data


def _refresh_bg(slug: str) -> None:
    try:
        _refresh(slug)
    except Exception as e:  # noqa: BLE001
        LOG.exception("list refresh %s failed: %s", slug, e)
        with _list_lock:
            _in_flight.discard(slug)


def get_list(slug: str, allow_stale: bool = True, force: bool = False) -> dict:
    """Stale-while-revalidate read of one list."""
    with _list_lock:
        hit = _list_cache.get(slug)
        age = time.time() - hit["generated_at"] if hit else None
        if hit and not force and age < LIST_TTL:
            return dict(hit, age_sec=int(age), stale=False)
        if hit and not force and allow_stale:
            if slug not in _in_flight:
                _in_flight.add(slug)
                _bg_pool.submit(_refresh_bg, slug)
            return dict(hit, age_sec=int(age), stale=True)
    data = _refresh(slug)
    return dict(data, age_sec=0, stale=False)


def hub() -> dict:
    """Cheap overview for the landing page: top-3 preview per list (raw
    mention data only, no enrichment) and a cross-source 'hot right now'."""
    previews: dict[str, list[dict]] = {}
    appearances: dict[str, dict] = {}

    def preview(slug: str) -> list[dict]:
        cfg = LISTS[slug]
        if cfg["kind"] == "ape":
            return _ape_rows(cfg)
        if cfg["kind"] == "stocktwits":
            return _stocktwits_rows(cfg)
        return []

    slugs = [s for s, c in LISTS.items() if c["kind"] in ("ape", "stocktwits")]
    with ThreadPoolExecutor(max_workers=6) as ex:
        results = dict(zip(slugs, ex.map(preview, slugs)))

    for slug, rows in results.items():
        previews[slug] = [{"symbol": r["symbol"], "name": r["name"]}
                          for r in rows[:3]]
        if LISTS[slug]["group"] != "social":
            continue
        for rank, r in enumerate(rows[:25]):
            a = appearances.setdefault(r["symbol"], {
                "symbol": r["symbol"], "name": r["name"], "lists": [],
                "heat": 0.0, "mentions": 0, "mentions_prev": 0})
            a["lists"].append(LISTS[slug]["short"])
            a["heat"] += (25 - rank) / 25
            # WSB is a subset of the all-Reddit feed, so don't double count.
            if r.get("metric") == "mentions" and slug != "wallstreetbets":
                a["mentions"] += r["mentions"]
                a["mentions_prev"] += r["mentions_prev"]

    # Derived previews borrow from the cached full list when we have it.
    for slug, cfg in LISTS.items():
        if slug in previews:
            continue
        cached = _list_cache.get(slug)
        previews[slug] = ([{"symbol": r["symbol"], "name": r["name"]}
                           for r in cached["rows"][:3]] if cached else [])

    hot = sorted(appearances.values(), key=lambda a: a["heat"], reverse=True)[:8]
    hot_syms = [h["symbol"] for h in hot]
    scored = {r["symbol"]: r for r in _enrich(
        [{"symbol": h["symbol"], "name": h["name"], "mentions": h["mentions"],
          "mentions_prev": h["mentions_prev"], "metric": "mentions"}
         for h in hot])}
    for h in hot:
        s = scored.get(h["symbol"]) or {}
        h.update({k: s.get(k) for k in ("score", "price", "change_pct",
                                        "sentiment", "logo",
                                        "mention_change_pct")})
        if s.get("name"):
            h["name"] = s["name"]

    lists = [{"slug": s, **{k: c[k] for k in ("title", "desc", "group",
                                               "icon", "short")},
              "preview": previews.get(s, [])} for s, c in LISTS.items()]
    return {"hot": hot, "hot_symbols": hot_syms, "lists": lists,
            "groups": [{"id": g, "title": t, "desc": d} for g, t, d in GROUPS],
            "generated_at": int(time.time())}


_hub_cache: dict = {}


def get_hub() -> dict:
    hit = _hub_cache.get("v")
    if hit and time.time() - hit["generated_at"] < LIST_TTL:
        return hit
    data = hub()
    _hub_cache["v"] = data
    return data

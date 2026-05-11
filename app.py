"""Flask app exposing the social-stock-sentiment dashboard."""

from __future__ import annotations

import logging
import threading
import time

from flask import Flask, jsonify, render_template, request

import aggregator
import catalysts
import db
import earnings as earnings_mod
import insider as insider_mod
import prices
import sentiment

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
LOG = logging.getLogger("app")

app = Flask(__name__)

CACHE_TTL_SEC = 5 * 60
# Per-window cache so the time-window selector doesn't trigger a fresh
# scrape every click.
_cache: dict[str, dict] = {}
_lock = threading.Lock()


def _refresh_locked(window: str) -> dict:
    LOG.info("refreshing aggregated data (window=%s)", window)
    data = aggregator.run(top_n=20, window=window)
    _cache[window] = {"data": data, "fetched_at": time.time()}
    return data


def get_data(force: bool = False, window: str = "now") -> dict:
    with _lock:
        entry = _cache.get(window)
        if force or entry is None or \
                (time.time() - entry["fetched_at"]) > CACHE_TTL_SEC:
            return _refresh_locked(window)
        return entry["data"]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ticker/<symbol>")
def ticker_page(symbol: str):
    return render_template("ticker.html", symbol=symbol.upper())


@app.route("/watchlist")
def watchlist_page():
    return render_template("watchlist.html")


@app.route("/earnings")
def earnings_page():
    return render_template("earnings.html")


@app.route("/insider")
def insider_page():
    return render_template("insider.html")


@app.route("/api/trending")
def trending():
    window = request.args.get("window", "now")
    if window not in ("now", "1h", "4h", "24h", "7d"):
        window = "now"
    return jsonify(get_data(window=window))


@app.route("/api/refresh", methods=["POST"])
def refresh():
    window = request.args.get("window", "now")
    if window not in ("now", "1h", "4h", "24h", "7d"):
        window = "now"
    return jsonify(get_data(force=True, window=window))


@app.route("/api/earnings")
def earnings_api():
    days = int(request.args.get("days", "14"))
    return jsonify({"items": earnings_mod.upcoming(days=days)})


@app.route("/api/insider")
def insider_api():
    view = request.args.get("view", "purchases")
    limit = int(request.args.get("limit", "50"))
    if view == "clusters":
        return jsonify({"items": insider_mod.cluster_buys(limit=limit)})
    return jsonify({"items": insider_mod.purchases(limit=limit)})


@app.route("/api/ticker/<symbol>")
def ticker_detail(symbol: str):
    sym = symbol.upper()
    now = int(time.time())
    snapshot = get_data()
    row = next((r for r in snapshot.get("tickers", []) if r["symbol"] == sym), None)

    history = db.history(sym, since_ts=now - 7 * 86400)
    messages = db.recent_messages(sym, limit=50)

    if row is None:
        # Ticker isn't in the current top-20 -- still surface anything we have.
        cat = catalysts.fetch([sym]).get(sym, {})
        row = {
            "symbol": sym,
            "mentions": history[-1]["mentions"] if history else 0,
            "avg_sentiment": history[-1]["avg_sentiment"] if history else 0.0,
            "trend": "neutral",
            "price": history[-1]["price"] if history else None,
            "change_pct": history[-1]["change_pct"] if history else None,
            "earnings_date": cat.get("earnings_date"),
            "earnings_days_out": cat.get("earnings_days_out"),
            "news": cat.get("news") or [],
            "in_top20": False,
        }
    else:
        row = dict(row)
        row["in_top20"] = True

    return jsonify({
        "row": row,
        "history": history,
        "messages": messages,
        "watchlist": db.watchlist_get(),
        "sentiment_backend": sentiment.backend(),
    })


@app.route("/api/watchlist", methods=["GET"])
def watchlist_get():
    return jsonify({"watchlist": db.watchlist_get()})


@app.route("/api/watchlist/data", methods=["POST"])
def watchlist_data():
    """Return enriched data for an arbitrary list of symbols.

    Body: {"symbols": ["AAPL", "TSLA", ...]}

    For each symbol we return: latest price/change %, latest mentions/
    sentiment we have (from the current snapshot if it's in top-20, else
    from DB history if available), and the catalyst summary.
    """
    body = request.get_json(silent=True) or {}
    syms = [s.strip().upper() for s in (body.get("symbols") or []) if s.strip()]
    syms = [s for s in syms if s.replace(".", "").isalnum()]
    syms = list(dict.fromkeys(syms))[:50]      # dedupe, cap to 50

    if not syms:
        return jsonify({"tickers": []})

    snap = get_data()
    in_top = {r["symbol"]: r for r in snap.get("tickers", [])}

    # Backfill price + catalysts in parallel for symbols not in the snapshot
    missing = [s for s in syms if s not in in_top]
    quotes = prices.fetch_quotes(missing) if missing else {}
    cats = catalysts.fetch(missing) if missing else {}

    now = int(time.time())
    out: list[dict] = []
    for sym in syms:
        if sym in in_top:
            r = dict(in_top[sym])
            r["in_top20"] = True
            out.append(r)
            continue
        # Fall back to DB's most-recent snapshot if we have one
        hist = db.history(sym, since_ts=now - 7 * 86400)
        last = hist[-1] if hist else None
        q = quotes.get(sym, {})
        c = cats.get(sym, {})
        out.append({
            "symbol": sym,
            "mentions": last["mentions"] if last else 0,
            "bullish": last.get("bullish", 0) if last else 0,
            "bearish": last.get("bearish", 0) if last else 0,
            "neutral": last.get("neutral", 0) if last else 0,
            "avg_sentiment": last["avg_sentiment"] if last else 0.0,
            "trend": sentiment.label(last["avg_sentiment"]) if last else "neutral",
            "sources": {},
            "price": q.get("price"),
            "change_pct": q.get("change_pct"),
            "currency": q.get("currency", ""),
            "earnings_date": c.get("earnings_date"),
            "earnings_days_out": c.get("earnings_days_out"),
            "news": c.get("news") or [],
            "catalyst_summary": catalysts.summary_label(c),
            "in_top20": False,
        })
    return jsonify({"tickers": out, "generated_at": snap.get("generated_at", now)})


@app.route("/api/watchlist", methods=["POST"])
def watchlist_add():
    body = request.get_json(silent=True) or {}
    sym = (body.get("symbol") or "").strip().upper()
    if not sym or not sym.replace(".", "").isalnum():
        return jsonify({"error": "invalid symbol"}), 400
    db.watchlist_add(sym)
    return jsonify({"watchlist": db.watchlist_get()})


@app.route("/api/watchlist/<symbol>", methods=["DELETE"])
def watchlist_delete(symbol: str):
    db.watchlist_remove(symbol.upper())
    return jsonify({"watchlist": db.watchlist_get()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)

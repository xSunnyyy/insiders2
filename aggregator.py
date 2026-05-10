"""Aggregate scraped items into per-ticker mention/sentiment summaries.

Pipeline per refresh:
    1. scrape Reddit / Stocktwits / Bluesky (+ Twitter if configured)
    2. extract tickers, score sentiment, weight by engagement
    3. enrich top-N with quotes + catalysts (earnings/news)
    4. enrich top-N with deltas vs prior history (1h/24h, sparkline, baseline z)
    5. persist snapshot + sample messages to SQLite
    6. evaluate alert rules and dispatch
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field

import alerts
import catalysts
import db
from prices import fetch_quotes
from scrapers import bluesky, reddit, stocktwits, twitter
from sentiment import label, score
from tickers import extract_tickers

LOG = logging.getLogger(__name__)

# Reddit comment fetching is by far the slowest stage (9 subs * 10 posts * 1
# request each, plus rate-limit sleeps). Off by default so the full pipeline
# fits comfortably in serverless timeouts; override on long-running hosts.
FETCH_COMMENTS = os.environ.get("REDDIT_FETCH_COMMENTS", "0") == "1"
REDDIT_PER_SUB = int(os.environ.get("REDDIT_PER_SUB", "30"))


@dataclass
class TickerStats:
    symbol: str
    mentions: int = 0
    bullish: int = 0
    bearish: int = 0
    neutral: int = 0
    weighted_sum: float = 0.0
    weight_total: float = 0.0
    sources: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    samples: list[dict] = field(default_factory=list)  # for drill-down

    def add(self, sentiment: float, weight: float, source: str,
            url: str, text: str, item_score: int):
        self.mentions += 1
        self.weighted_sum += sentiment * weight
        self.weight_total += weight
        if sentiment >= 0.15:
            self.bullish += 1
        elif sentiment <= -0.15:
            self.bearish += 1
        else:
            self.neutral += 1
        self.sources[source] += 1
        # Keep up to 10 highest-weighted samples for the drill-down view.
        if len(self.samples) < 10:
            self.samples.append({
                "source": source, "url": url, "text": text[:600],
                "score": item_score, "sentiment": sentiment, "weight": weight,
            })

    @property
    def avg_sentiment(self) -> float:
        if self.weight_total == 0:
            return 0.0
        return self.weighted_sum / self.weight_total

    @property
    def trend(self) -> str:
        return label(self.avg_sentiment)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "mentions": self.mentions,
            "bullish": self.bullish,
            "bearish": self.bearish,
            "neutral": self.neutral,
            "avg_sentiment": round(self.avg_sentiment, 3),
            "trend": self.trend,
            "sources": dict(self.sources),
        }


def _explicit_to_score(v: str | None) -> float | None:
    if v == "Bullish":
        return 1.0
    if v == "Bearish":
        return -1.0
    return None


def _process(items: list[dict], stats: dict[str, TickerStats]) -> None:
    for it in items:
        text = f"{it.get('title', '')} {it.get('body', '')}".strip()
        explicit = _explicit_to_score(it.get("explicit_sentiment"))
        sent = explicit if explicit is not None else score(text)
        syms = set(it.get("symbols") or []) or extract_tickers(text)
        if not syms:
            continue
        weight = 1.0 + min(max(it.get("score", 0), 0), 1000) / 100.0
        for sym in syms:
            stat = stats.get(sym)
            if stat is None:
                stat = stats[sym] = TickerStats(symbol=sym)
            stat.add(sent, weight, it.get("source", "?"),
                     it.get("url", ""), text, int(it.get("score", 0) or 0))


def _scrape_all() -> tuple[dict[str, TickerStats], list[str]]:
    stats: dict[str, TickerStats] = {}
    sources_used: list[str] = []

    try:
        reddit_items = reddit.fetch_all(
            per_sub=REDDIT_PER_SUB,
            include_comments=FETCH_COMMENTS,
        )
        err = reddit.last_error()
        if reddit_items:
            tag = " via pullpush" if err and err.startswith("4") else ""
            sources_used.append(f"reddit({len(reddit_items)}{tag})")
        elif err:
            sources_used.append(f"reddit(0; {err})")
        else:
            sources_used.append("reddit(0)")
        _process(reddit_items, stats)
    except Exception as e:
        LOG.exception("reddit scrape failed: %s", e)
        sources_used.append(f"reddit(error: {e})")

    try:
        st_items = stocktwits.fetch_all()
        sources_used.append(f"stocktwits({len(st_items)})")
        _process(st_items, stats)
    except Exception as e:
        LOG.exception("stocktwits scrape failed: %s", e)

    try:
        bs_items = bluesky.fetch_all()
        sources_used.append(f"bluesky({len(bs_items)})")
        _process(bs_items, stats)
    except Exception as e:
        LOG.exception("bluesky scrape failed: %s", e)

    if twitter.is_enabled():
        try:
            tw_items = twitter.fetch_all()
            sources_used.append(f"twitter({len(tw_items)})")
            _process(tw_items, stats)
        except Exception as e:
            LOG.exception("twitter scrape failed: %s", e)
    else:
        sources_used.append("twitter(disabled)")

    return stats, sources_used


def _enrich_with_history(rows: list[dict], ts: int) -> None:
    """Add prior_mentions_*, deltas, baseline stats, sparkline."""
    for r in rows:
        sym = r["symbol"]
        m_now = r["mentions"]
        m_1h = db.prior_mentions(sym, ts, 3600)
        m_24h = db.prior_mentions(sym, ts, 24 * 3600)
        ps_24 = db.prior_sentiment(sym, ts, 24 * 3600)
        mean, std = db.baseline_mentions(sym, ts, days=14)
        z = ((m_now - mean) / std) if std > 0 else None
        r["delta_mentions_1h"] = m_now - m_1h
        r["delta_mentions_24h"] = m_now - m_24h
        r["prior_sentiment_24h"] = ps_24
        r["delta_sentiment_24h"] = (
            round(r["avg_sentiment"] - ps_24, 3) if ps_24 is not None else None
        )
        r["baseline_mean"] = round(mean, 2) if mean else 0.0
        r["baseline_std"] = round(std, 2) if std else 0.0
        r["z_score"] = round(z, 2) if z is not None else None
        r["sparkline"] = db.sparkline(sym, ts, hours=24, points=24)


def _persist(rows: list[dict], stats: dict[str, TickerStats], ts: int) -> None:
    db.write_snapshot(rows, ts=ts)
    msgs: list[dict] = []
    for r in rows:
        sym = r["symbol"]
        stat = stats.get(sym)
        if not stat:
            continue
        for s in stat.samples:
            msgs.append({
                "ticker": sym,
                "source": s.get("source"),
                "text": s.get("text"),
                "score": s.get("score"),
                "url": s.get("url"),
                "sentiment": s.get("sentiment"),
            })
    if msgs:
        db.write_messages(msgs, ts=ts)


def run(top_n: int = 20) -> dict:
    """Full pipeline. Returns the dashboard payload."""
    started = time.time()
    ts = int(started)

    stats, sources_used = _scrape_all()

    ranked = sorted(stats.values(), key=lambda s: s.mentions, reverse=True)[:top_n]
    rows = [s.to_dict() for s in ranked]

    # Quotes (Yahoo)
    quotes = fetch_quotes([r["symbol"] for r in rows])
    for r in rows:
        q = quotes.get(r["symbol"])
        r["price"] = q["price"] if q else None
        r["change_pct"] = q["change_pct"] if q else None
        r["currency"] = q["currency"] if q else ""

    # Catalysts (earnings + news)
    cat = catalysts.fetch([r["symbol"] for r in rows])
    for r in rows:
        c = cat.get(r["symbol"], {})
        r["earnings_date"] = c.get("earnings_date")
        r["earnings_days_out"] = c.get("earnings_days_out")
        r["news"] = c.get("news") or []
        r["catalyst_summary"] = catalysts.summary_label(c)

    # Deltas + sparkline (uses prior snapshots)
    _enrich_with_history(rows, ts)

    # Persist this snapshot before evaluating alerts so the snapshot
    # itself becomes part of history for the *next* run.
    try:
        _persist(rows, stats, ts)
    except Exception as e:
        LOG.exception("persistence failed: %s", e)

    # Alerts
    try:
        if rows:
            alerts.evaluate(rows, ts)
    except Exception as e:
        LOG.exception("alert dispatch failed: %s", e)

    # Housekeeping
    try:
        db.prune_snapshots(older_than_days=30)
        db.prune_messages(older_than_days=7)
    except Exception as e:
        LOG.warning("prune failed: %s", e)

    return {
        "generated_at": ts,
        "duration_sec": round(time.time() - started, 2),
        "sources": sources_used,
        "twitter_enabled": twitter.is_enabled(),
        "alert_channels": alerts.channels_configured(),
        "watchlist": db.watchlist_get(),
        "tickers": rows,
    }

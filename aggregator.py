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
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import alerts
import catalysts
import db
import sectors
from prices import fetch_quotes
from scrapers import bluesky, reddit, stocktwits, twitter
from sentiment import label, score
from tickers import extract_tickers


SOURCE_CATEGORIES = ("reddit", "stocktwits", "bluesky", "twitter")


def _category(source: str) -> str:
    """Normalize a granular source label (e.g. 'reddit/r/wsb') to a category."""
    s = (source or "").lower()
    for cat in SOURCE_CATEGORIES:
        if s.startswith(cat):
            return cat
    return "other"

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
    # Per-source rollups: bullish/bearish/total + weighted sentiment sum.
    per_source: dict[str, dict] = field(default_factory=lambda: defaultdict(
        lambda: {"mentions": 0, "bullish": 0, "bearish": 0,
                 "neutral": 0, "wsum": 0.0, "wtot": 0.0}))
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

        cat = _category(source)
        ps = self.per_source[cat]
        ps["mentions"] += 1
        ps["wsum"] += sentiment * weight
        ps["wtot"] += weight
        if sentiment >= 0.15:
            ps["bullish"] += 1
        elif sentiment <= -0.15:
            ps["bearish"] += 1
        else:
            ps["neutral"] += 1

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

    def per_source_trends(self) -> dict[str, str]:
        """Return {category: 'bullish'|'bearish'|'neutral'} per active source."""
        out: dict[str, str] = {}
        for cat, ps in self.per_source.items():
            if ps["mentions"] == 0:
                continue
            avg = ps["wsum"] / ps["wtot"] if ps["wtot"] else 0.0
            out[cat] = label(avg)
        return out

    def to_dict(self) -> dict:
        ts = self.per_source_trends()
        # Consensus = fraction of contributing sources whose trend matches the
        # majority trend. 1.0 = all sources agree, 0.5 = split.
        trends = list(ts.values())
        consensus = 0.0
        consensus_label = "none"
        if trends:
            from collections import Counter
            c = Counter(trends)
            top, top_n = c.most_common(1)[0]
            consensus = round(top_n / len(trends), 2)
            consensus_label = top
        return {
            "symbol": self.symbol,
            "mentions": self.mentions,
            "bullish": self.bullish,
            "bearish": self.bearish,
            "neutral": self.neutral,
            "avg_sentiment": round(self.avg_sentiment, 3),
            "trend": self.trend,
            "sources": dict(self.sources),
            "per_source_trends": ts,
            "consensus": consensus,
            "consensus_label": consensus_label,
            "source_count": len(ts),
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
        # Stocktwits emits crypto pairs as e.g. "BTC.X" / "ETH.X". They're
        # not equities and won't resolve to prices; drop them so they
        # don't crowd the equity rankings.
        syms = {s for s in syms if not s.endswith(".X")}
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
    """Run the four social-media scrapes concurrently. Each is network-bound
    so threads are fine and we save ~the sum of the slowest stage."""
    stats: dict[str, TickerStats] = {}
    sources_used: list[str] = []
    t0 = time.time()

    def _reddit():
        return reddit.fetch_all(per_sub=REDDIT_PER_SUB,
                                include_comments=FETCH_COMMENTS)
    def _stocktwits(): return stocktwits.fetch_all()
    def _bluesky():    return bluesky.fetch_all()
    def _twitter():    return twitter.fetch_all() if twitter.is_enabled() else []

    with ThreadPoolExecutor(max_workers=4) as ex:
        f_r = ex.submit(_reddit)
        f_s = ex.submit(_stocktwits)
        f_b = ex.submit(_bluesky)
        f_t = ex.submit(_twitter)

        # Reddit
        try:
            reddit_items = f_r.result()
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
            st_items = f_s.result()
            sources_used.append(f"stocktwits({len(st_items)})")
            _process(st_items, stats)
        except Exception as e:
            LOG.exception("stocktwits scrape failed: %s", e)
            sources_used.append(f"stocktwits(error: {e})")

        try:
            bs_items = f_b.result()
            sources_used.append(f"bluesky({len(bs_items)})")
            _process(bs_items, stats)
        except Exception as e:
            LOG.exception("bluesky scrape failed: %s", e)
            sources_used.append(f"bluesky(error: {e})")

        if twitter.is_enabled():
            try:
                tw_items = f_t.result()
                sources_used.append(f"twitter({len(tw_items)})")
                _process(tw_items, stats)
            except Exception as e:
                LOG.exception("twitter scrape failed: %s", e)
        else:
            sources_used.append("twitter(disabled)")

    LOG.info("scrape stage took %.1fs (parallel)", time.time() - t0)
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


def _compute_risk_flags(row: dict) -> list[str]:
    """Heuristic flags to surface obvious pump / coordinated patterns."""
    flags: list[str] = []
    mcap = row.get("market_cap")
    if mcap is not None and mcap < 500_000_000 and row.get("mentions", 0) >= 10:
        flags.append("small_cap")
    if row.get("z_score") is not None and row["z_score"] >= 3 \
            and not row.get("earnings_days_out") in range(-3, 8):
        flags.append("uncaused_spike")
    total = row.get("bullish", 0) + row.get("bearish", 0) + row.get("neutral", 0)
    if total >= 20 and row.get("bullish", 0) / max(total, 1) >= 0.9:
        flags.append("euphoria")
    if row.get("source_count", 0) == 1 and row.get("mentions", 0) >= 20:
        flags.append("single_source")
    return flags


def run(top_n: int = 20, window: str = "now") -> dict:
    """Full pipeline. Returns the dashboard payload.

    `window` controls the ranking:
        'now'  current refresh only (default)
        '1h'   peak mentions in trailing 1 hour
        '4h'   trailing 4 hours
        '24h'  trailing 24 hours
        '7d'   trailing 7 days
    Time windows other than 'now' rank against persisted snapshots and
    will be empty until enough history has accumulated.
    """
    started = time.time()
    ts = int(started)

    stats, sources_used = _scrape_all()

    # Always do a current-refresh ranking so the dashboard has something
    # even on a fresh DB.
    ranked_now = sorted(stats.values(), key=lambda s: s.mentions, reverse=True)
    rows_now = [s.to_dict() for s in ranked_now[:top_n]]

    if window == "now":
        rows = rows_now
    else:
        win_sec = {"1h": 3600, "4h": 4*3600, "24h": 24*3600, "7d": 7*86400}.get(window, 3600)
        hist_rows = db.aggregate_window(ts - win_sec, ts, top_n=top_n)
        # Map db.aggregate_window output -> dashboard schema
        by_now = {r["symbol"]: r for r in rows_now}
        rows = []
        for h in hist_rows:
            sym = h["ticker"]
            base = by_now.get(sym, {})
            rows.append({
                "symbol": sym,
                "mentions": int(h["peak_mentions"] or 0),
                "bullish": int(h["bullish"] or 0),
                "bearish": int(h["bearish"] or 0),
                "neutral": int(h["neutral"] or 0),
                "avg_sentiment": round(float(h["avg_sentiment"] or 0.0), 3),
                "trend": label(float(h["avg_sentiment"] or 0.0)),
                "sources": h.get("sources") or base.get("sources") or {},
                "per_source_trends": base.get("per_source_trends", {}),
                "consensus": base.get("consensus", 0.0),
                "consensus_label": base.get("consensus_label", "none"),
                "source_count": base.get("source_count", 0),
            })
        # Fall back to current if history is empty (e.g. fresh deploy)
        if not rows:
            rows = rows_now

    # Enrich top-N in parallel: quotes, sectors, catalysts are all
    # network-bound and independent.
    syms_list = [r["symbol"] for r in rows]
    t_enrich = time.time()
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_q = ex.submit(fetch_quotes, syms_list)
        f_s = ex.submit(sectors.fetch, syms_list)
        f_c = ex.submit(catalysts.fetch, syms_list)
        quotes = f_q.result()
        secs = f_s.result()
        cat = f_c.result()
    LOG.info("enrich stage took %.1fs (parallel)", time.time() - t_enrich)

    for r in rows:
        sym = r["symbol"]
        q = quotes.get(sym) or {}
        r["price"] = q.get("price")
        r["change_pct"] = q.get("change_pct")
        r["currency"] = q.get("currency", "")
        r["price_source"] = q.get("source", "")
        r["pre_price"] = q.get("pre_price")
        r["pre_change_pct"] = q.get("pre_change_pct")
        r["post_price"] = q.get("post_price")
        r["post_change_pct"] = q.get("post_change_pct")

        s = secs.get(sym) or {}
        r["sector"] = s.get("sector") or ""
        r["industry"] = s.get("industry") or ""
        r["market_cap"] = s.get("market_cap")

        c = cat.get(sym, {})
        r["earnings_date"] = c.get("earnings_date")
        r["earnings_days_out"] = c.get("earnings_days_out")
        r["news"] = c.get("news") or []
        r["catalyst_summary"] = catalysts.summary_label(c)

    # Deltas + sparkline (uses prior snapshots)
    _enrich_with_history(rows, ts)

    # Risk flags depend on prior enrichment (z_score, market_cap, earnings)
    for r in rows:
        r["risk_flags"] = _compute_risk_flags(r)

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

    # Sector rollup over the rows in view: mean weighted sentiment + count.
    sector_roll: dict[str, dict] = {}
    for r in rows:
        sec = (r.get("sector") or "").strip()
        if not sec:
            continue
        agg = sector_roll.setdefault(sec, {"count": 0, "wsum": 0.0, "wtot": 0.0,
                                            "tickers": []})
        agg["count"] += 1
        agg["wsum"] += r["avg_sentiment"] * max(r["mentions"], 1)
        agg["wtot"] += max(r["mentions"], 1)
        agg["tickers"].append(r["symbol"])
    sector_summary = []
    for sec, a in sector_roll.items():
        avg = a["wsum"] / a["wtot"] if a["wtot"] else 0.0
        sector_summary.append({
            "sector": sec, "count": a["count"],
            "avg_sentiment": round(avg, 3),
            "trend": label(avg),
            "tickers": a["tickers"],
        })
    sector_summary.sort(key=lambda x: (-x["count"], -x["avg_sentiment"]))

    return {
        "generated_at": ts,
        "window": window,
        "duration_sec": round(time.time() - started, 2),
        "sources": sources_used,
        "twitter_enabled": twitter.is_enabled(),
        "alert_channels": alerts.channels_configured(),
        "watchlist": db.watchlist_get(),
        "tickers": rows,
        "sector_summary": sector_summary,
    }

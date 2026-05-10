"""Aggregate scraped items into per-ticker mention/sentiment summaries."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field

from scrapers import bluesky, reddit, stocktwits, twitter
from sentiment import label, score
from tickers import extract_tickers

LOG = logging.getLogger(__name__)


@dataclass
class TickerStats:
    symbol: str
    mentions: int = 0
    bullish: int = 0
    bearish: int = 0
    neutral: int = 0
    score_sum: float = 0.0
    weighted_sum: float = 0.0
    weight_total: float = 0.0
    sources: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    sample_urls: list[str] = field(default_factory=list)

    def add(self, sentiment: float, weight: float, source: str, url: str):
        self.mentions += 1
        self.score_sum += sentiment
        self.weighted_sum += sentiment * weight
        self.weight_total += weight
        if sentiment >= 0.15:
            self.bullish += 1
        elif sentiment <= -0.15:
            self.bearish += 1
        else:
            self.neutral += 1
        self.sources[source] += 1
        if url and len(self.sample_urls) < 3 and url not in self.sample_urls:
            self.sample_urls.append(url)

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
            "sample_urls": self.sample_urls,
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
        # Prefer symbols already attached by the source (Stocktwits), else extract.
        syms = set(it.get("symbols") or []) or extract_tickers(text)
        if not syms:
            continue
        # Weight by upvotes/likes so a viral post counts more than a one-off.
        weight = 1.0 + min(max(it.get("score", 0), 0), 1000) / 100.0
        for sym in syms:
            stat = stats.get(sym)
            if stat is None:
                stat = stats[sym] = TickerStats(symbol=sym)
            stat.add(sent, weight, it.get("source", "?"), it.get("url", ""))


def run(top_n: int = 20) -> dict:
    """Scrape all sources and return ranked top-N ticker summaries."""
    started = time.time()
    stats: dict[str, TickerStats] = {}

    sources_used: list[str] = []
    try:
        reddit_items = reddit.fetch_all()
        sources_used.append(f"reddit({len(reddit_items)})")
        _process(reddit_items, stats)
    except Exception as e:
        LOG.exception("reddit scrape failed: %s", e)

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

    ranked = sorted(stats.values(), key=lambda s: s.mentions, reverse=True)[:top_n]
    return {
        "generated_at": int(started),
        "duration_sec": round(time.time() - started, 2),
        "sources": sources_used,
        "twitter_enabled": twitter.is_enabled(),
        "tickers": [s.to_dict() for s in ranked],
    }

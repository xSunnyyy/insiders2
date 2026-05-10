"""Reddit scraper using the public JSON endpoints (no auth required for read).

Pulls hot/new posts and their top-level comments from finance subreddits.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

import requests

LOG = logging.getLogger(__name__)

DEFAULT_SUBS = (
    "wallstreetbets",
    "stocks",
    "investing",
    "StockMarket",
    "options",
    "pennystocks",
)

UA = "stock-sentiment-bot/0.1 (read-only public JSON)"


def _get(url: str, params: dict | None = None) -> dict | None:
    try:
        r = requests.get(url, params=params, headers={"User-Agent": UA}, timeout=10)
        if r.status_code != 200:
            LOG.warning("reddit %s -> %s", url, r.status_code)
            return None
        return r.json()
    except requests.RequestException as e:
        LOG.warning("reddit request failed: %s", e)
        return None


def fetch_subreddit(sub: str, listing: str = "hot", limit: int = 50) -> list[dict]:
    """Return [{title, body, score, url, source}] for posts in `sub`."""
    data = _get(f"https://www.reddit.com/r/{sub}/{listing}.json",
                params={"limit": limit})
    if not data:
        return []
    items: list[dict] = []
    for child in data.get("data", {}).get("children", []):
        d = child.get("data", {})
        items.append({
            "title": d.get("title", ""),
            "body": d.get("selftext", ""),
            "score": int(d.get("score", 0) or 0),
            "url": "https://reddit.com" + d.get("permalink", ""),
            "source": f"reddit/r/{sub}",
        })
    return items


def fetch_comments(permalink_url: str, limit: int = 50) -> list[dict]:
    """Return top-level comments from a post permalink."""
    json_url = permalink_url.rstrip("/") + ".json"
    data = _get(json_url, params={"limit": limit})
    if not isinstance(data, list) or len(data) < 2:
        return []
    out: list[dict] = []
    for child in data[1].get("data", {}).get("children", []):
        d = child.get("data", {})
        if d.get("body"):
            out.append({
                "title": "",
                "body": d["body"],
                "score": int(d.get("score", 0) or 0),
                "url": permalink_url,
                "source": "reddit/comment",
            })
    return out


def fetch_all(subs: Iterable[str] = DEFAULT_SUBS,
              per_sub: int = 50,
              include_comments: bool = True,
              comments_per_post: int = 20) -> list[dict]:
    """Fetch posts (and optionally comments) across the given subreddits."""
    results: list[dict] = []
    for sub in subs:
        posts = fetch_subreddit(sub, limit=per_sub)
        results.extend(posts)
        if include_comments:
            for p in posts[:10]:
                results.extend(fetch_comments(p["url"], limit=comments_per_post))
                time.sleep(0.5)
        time.sleep(1.0)
    LOG.info("reddit: collected %d items", len(results))
    return results

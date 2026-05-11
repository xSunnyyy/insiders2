"""Bluesky scraper using the unauthenticated public AppView.

The AppView at `public.api.bsky.app` exposes `app.bsky.feed.searchPosts`
without auth or an API key. Each post carries like/repost/reply counts that
we use as the engagement weight.
"""

from __future__ import annotations

import logging
import time

import requests

LOG = logging.getLogger(__name__)

BASE = "https://public.api.bsky.app/xrpc"
UA = "stock-sentiment-bot/0.1"

# A handful of broad queries to pick up stock chatter from multiple angles.
# Bluesky's searchPosts treats spaces as AND, so each entry is a separate
# request. Cashtags (`$SPY`) and hashtags (`#stocks`) both work.
QUERIES = (
    "stocks",
    "$SPY",
    "$QQQ",
    "earnings",
    "bullish",
    "bearish",
    "options trading",
    "wallstreetbets",
)


def _get(path: str, params: dict) -> dict | None:
    try:
        r = requests.get(f"{BASE}/{path}", params=params,
                         headers={"User-Agent": UA}, timeout=10)
        if r.status_code != 200:
            LOG.warning("bluesky %s -> %s: %s", path, r.status_code, r.text[:200])
            return None
        return r.json()
    except requests.RequestException as e:
        LOG.warning("bluesky request failed: %s", e)
        return None


def search_posts(query: str, limit: int = 100) -> list[dict]:
    data = _get("app.bsky.feed.searchPosts",
                {"q": query, "limit": min(max(limit, 1), 100), "lang": "en"})
    if not data:
        return []
    out: list[dict] = []
    for p in data.get("posts", []):
        rec = p.get("record") or {}
        text = rec.get("text", "") or ""
        if not text:
            continue
        author = (p.get("author") or {}).get("handle") or ""
        # Convert at:// uri to a https://bsky.app permalink when possible.
        uri = p.get("uri") or ""
        url = ""
        if uri.startswith("at://") and author:
            rkey = uri.rsplit("/", 1)[-1]
            url = f"https://bsky.app/profile/{author}/post/{rkey}"
        likes = int(p.get("likeCount") or 0)
        reposts = int(p.get("repostCount") or 0)
        out.append({
            "title": "",
            "body": text,
            "score": likes + reposts,
            "url": url,
            "source": "bluesky",
        })
    return out


def fetch_all(per_query: int = 50) -> list[dict]:
    """Run each search query in parallel and dedupe by post URL."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    items: list[dict] = []
    seen_urls: set[str] = set()
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(search_posts, q, per_query): q for q in QUERIES}
        for fut in as_completed(futures):
            try:
                for it in fut.result() or []:
                    u = it.get("url") or it.get("body", "")[:80]
                    if u in seen_urls:
                        continue
                    seen_urls.add(u)
                    items.append(it)
            except Exception as e:
                LOG.warning("bluesky %s: %s", futures[fut], e)
    LOG.info("bluesky: collected %d unique posts across %d queries",
             len(items), len(QUERIES))
    return items

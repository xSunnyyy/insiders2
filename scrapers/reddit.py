"""Reddit scraper.

Reddit's unauthenticated `.json` endpoints have been heavily rate-limited and
often outright blocked since the 2023 API changes -- especially from
datacenter IPs and generic user-agents. The reliable path is OAuth:

    Set REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, and REDDIT_USER_AGENT
    (e.g. "linux:stock-sentiment:0.1 (by /u/yourname)").

When those are present we use https://oauth.reddit.com with an app-only
bearer token (free; no Reddit user login needed if you create a "script"
app at https://www.reddit.com/prefs/apps).

Without credentials we still try the public `.json` endpoints with a
spec-compliant UA -- this works on residential IPs at low volume but is
unreliable from cloud hosts. Failures are surfaced via `last_error()`.
"""

from __future__ import annotations

import logging
import os
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
    "Daytrading",
    "Superstonk",
    "stock_picks",
)

DEFAULT_UA = os.environ.get(
    "REDDIT_USER_AGENT",
    "linux:stock-sentiment-bot:0.1 (by /u/anon)",
)

_token_cache: dict = {"token": None, "expires_at": 0.0}
_last_error: str | None = None


def last_error() -> str | None:
    """Most recent non-200 status or transport error from this scraper."""
    return _last_error


def _set_error(msg: str | None) -> None:
    global _last_error
    _last_error = msg


def _oauth_token() -> str | None:
    cid = os.environ.get("REDDIT_CLIENT_ID")
    secret = os.environ.get("REDDIT_CLIENT_SECRET")
    if not cid or not secret:
        return None
    if _token_cache["token"] and time.time() < _token_cache["expires_at"]:
        return _token_cache["token"]
    try:
        r = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(cid, secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": DEFAULT_UA},
            timeout=10,
        )
        if r.status_code != 200:
            LOG.warning("reddit oauth token failed: %s %s",
                        r.status_code, r.text[:200])
            _set_error(f"oauth token {r.status_code}")
            return None
        data = r.json()
        _token_cache["token"] = data.get("access_token")
        _token_cache["expires_at"] = time.time() + int(data.get("expires_in", 3600)) - 60
        return _token_cache["token"]
    except requests.RequestException as e:
        LOG.warning("reddit oauth token error: %s", e)
        _set_error(f"oauth token error: {e}")
        return None


def _get(path: str, params: dict | None = None) -> dict | list | None:
    """Fetch a Reddit JSON path. Tries OAuth first, falls back to public."""
    token = _oauth_token()
    if token:
        url = f"https://oauth.reddit.com{path}"
        headers = {"Authorization": f"Bearer {token}", "User-Agent": DEFAULT_UA}
    else:
        # Public JSON; path is something like "/r/wsb/hot.json".
        url = f"https://www.reddit.com{path}"
        headers = {"User-Agent": DEFAULT_UA}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code != 200:
            LOG.warning("reddit %s -> %s (auth=%s)", path, r.status_code,
                        bool(token))
            _set_error(f"{r.status_code}{' auth' if token else ' anon'}")
            return None
        return r.json()
    except requests.RequestException as e:
        LOG.warning("reddit request failed: %s", e)
        _set_error(f"transport error: {e}")
        return None


def fetch_subreddit(sub: str, listing: str = "hot", limit: int = 50) -> list[dict]:
    data = _get(f"/r/{sub}/{listing}.json", params={"limit": limit, "raw_json": 1})
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
            "permalink": d.get("permalink", ""),
            "source": f"reddit/r/{sub}",
        })
    return items


def fetch_comments(permalink: str, limit: int = 50) -> list[dict]:
    """Top-level comments for a post permalink (path form, e.g. /r/wsb/comments/...)."""
    if not permalink:
        return []
    path = permalink.rstrip("/") + ".json"
    data = _get(path, params={"limit": limit, "raw_json": 1})
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
                "url": "https://reddit.com" + permalink,
                "source": "reddit/comment",
            })
    return out


def fetch_all(subs: Iterable[str] = DEFAULT_SUBS,
              per_sub: int = 50,
              include_comments: bool = True,
              comments_per_post: int = 20) -> list[dict]:
    _set_error(None)
    results: list[dict] = []
    for sub in subs:
        posts = fetch_subreddit(sub, limit=per_sub)
        results.extend(posts)
        if include_comments:
            for p in posts[:10]:
                results.extend(fetch_comments(p.get("permalink", ""),
                                              limit=comments_per_post))
                time.sleep(0.5)
        time.sleep(1.0)
    LOG.info("reddit: collected %d items (last_error=%s)",
             len(results), _last_error)
    return results

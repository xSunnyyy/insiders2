"""Optional Twitter/X scraper.

Twitter no longer permits free anonymous scraping — `snscrape` and the
guest-token endpoints have been killed. To enable this source, set the
`TWITTER_BEARER_TOKEN` environment variable to a valid v2 API bearer token
(paid tier). Without it, this module yields no results and silently no-ops.
"""

from __future__ import annotations

import logging
import os

import requests

LOG = logging.getLogger(__name__)

QUERY = ("(stocks OR $SPY OR $QQQ OR earnings OR bullish OR bearish) "
         "lang:en -is:retweet")


def is_enabled() -> bool:
    return bool(os.environ.get("TWITTER_BEARER_TOKEN"))


def fetch_all(max_results: int = 100) -> list[dict]:
    token = os.environ.get("TWITTER_BEARER_TOKEN")
    if not token:
        LOG.info("twitter: disabled (no TWITTER_BEARER_TOKEN)")
        return []
    try:
        r = requests.get(
            "https://api.twitter.com/2/tweets/search/recent",
            headers={"Authorization": f"Bearer {token}"},
            params={"query": QUERY,
                    "max_results": min(max(max_results, 10), 100),
                    "tweet.fields": "public_metrics,lang"},
            timeout=15,
        )
        if r.status_code != 200:
            LOG.warning("twitter %s: %s", r.status_code, r.text[:200])
            return []
        data = r.json()
    except requests.RequestException as e:
        LOG.warning("twitter request failed: %s", e)
        return []

    out: list[dict] = []
    for t in data.get("data", []):
        m = t.get("public_metrics") or {}
        out.append({
            "title": "",
            "body": t.get("text", "") or "",
            "score": int(m.get("like_count", 0)) + int(m.get("retweet_count", 0)),
            "url": f"https://twitter.com/i/web/status/{t.get('id')}",
            "source": "twitter",
        })
    LOG.info("twitter: collected %d tweets", len(out))
    return out

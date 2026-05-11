"""Yahoo Finance crumb + cookie handshake.

Since 2023 Yahoo's quoteSummary endpoint rejects requests without a
crumb token (returns 401 "Invalid Crumb"). The handshake is:

    1. GET https://fc.yahoo.com  -> sets session cookies
    2. GET https://query1.finance.yahoo.com/v1/test/getcrumb
       with those cookies -> returns the crumb string

We cache the crumb + cookies for the process lifetime.
"""

from __future__ import annotations

import logging
import threading
import time

import requests

LOG = logging.getLogger(__name__)

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")

_lock = threading.Lock()
_session: requests.Session | None = None
_crumb: str | None = None
_fetched_at: float = 0.0
TTL_SEC = 60 * 60   # refresh hourly


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    # We share this session across ~24 parallel threads (quotes + sectors +
    # catalysts each launch their own pools); the default pool size of 10
    # triggers "Connection pool is full" warnings. Bump it.
    from requests.adapters import HTTPAdapter
    adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _refresh() -> None:
    global _session, _crumb, _fetched_at
    s = _build_session()
    try:
        s.get("https://fc.yahoo.com", timeout=8, allow_redirects=True)
        r = s.get("https://query1.finance.yahoo.com/v1/test/getcrumb",
                  timeout=8)
        if r.status_code == 200 and r.text and len(r.text) < 64:
            _session = s
            _crumb = r.text.strip()
            _fetched_at = time.time()
            LOG.info("yahoo crumb obtained")
            return
        LOG.warning("yahoo getcrumb -> %s %r", r.status_code, r.text[:80])
    except requests.RequestException as e:
        LOG.warning("yahoo crumb handshake failed: %s", e)
    # On failure, leave _session/_crumb unchanged so callers can still try.


def get() -> tuple[requests.Session, str | None]:
    """Return (session, crumb). Session is always non-None; crumb may be None
    if Yahoo wouldn't issue one (callers should still try, some endpoints
    work without)."""
    global _session
    with _lock:
        if _session is None or (time.time() - _fetched_at) > TTL_SEC:
            _refresh()
        if _session is None:
            _session = _build_session()
        return _session, _crumb

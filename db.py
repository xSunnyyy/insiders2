"""SQLite-backed history store.

One file, four tables:

  snapshots   one row per (refresh, ticker) -- mention/sentiment/price history
  messages    sample messages per ticker per refresh, for drill-down
  watchlist   tickers the user pinned
  alerts_log  alerts that have already fired (for dedupe)

Schema is created idempotently on first connection.

Read-only filesystem handling: on hosts like Vercel/AWS Lambda the project
directory is read-only. We default to `/tmp/data.sqlite3` (the only writable
path on those platforms) and silently downgrade to a no-op if even that
fails. Callers always get safe defaults; the dashboard's history columns
just stay empty.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Iterable

LOG = logging.getLogger(__name__)

# /tmp is writable on Vercel, AWS Lambda, GCP Cloud Functions, etc.
# Override with DB_PATH on long-running hosts that have a real disk.
DB_PATH = os.environ.get("DB_PATH", "/tmp/data.sqlite3")
_disabled: bool = False  # flips True if init() fails -- everything no-ops

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              INTEGER NOT NULL,
    ticker          TEXT NOT NULL,
    mentions        INTEGER NOT NULL,
    bullish         INTEGER NOT NULL,
    bearish         INTEGER NOT NULL,
    neutral         INTEGER NOT NULL,
    avg_sentiment   REAL NOT NULL,
    price           REAL,
    change_pct      REAL,
    sources_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_snapshots_ticker_ts ON snapshots(ticker, ts);
CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(ts);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              INTEGER NOT NULL,
    ticker          TEXT NOT NULL,
    source          TEXT,
    text            TEXT,
    score           INTEGER,
    url             TEXT,
    sentiment       REAL
);
CREATE INDEX IF NOT EXISTS idx_messages_ticker_ts ON messages(ticker, ts);

CREATE TABLE IF NOT EXISTS watchlist (
    symbol          TEXT PRIMARY KEY,
    added_at        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              INTEGER NOT NULL,
    ticker          TEXT NOT NULL,
    kind            TEXT NOT NULL,
    payload_json    TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_ticker_kind ON alerts_log(ticker, kind, ts);
"""


@contextmanager
def connect():
    """Yield a sqlite connection, or None if storage is disabled."""
    if _disabled:
        yield None
        return
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
    except sqlite3.Error as e:
        LOG.warning("sqlite connect failed (%s); disabling storage", e)
        _disable()
        yield None
        return
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except sqlite3.Error as e:
        LOG.warning("sqlite op failed: %s", e)
    finally:
        conn.close()


def _disable() -> None:
    global _disabled
    _disabled = True


def is_enabled() -> bool:
    return not _disabled


def init() -> None:
    """Create schema. Disables storage on any failure; never raises."""
    try:
        with connect() as c:
            if c is None:
                return
            c.executescript(SCHEMA)
    except sqlite3.Error as e:
        LOG.warning("sqlite init failed (%s); disabling storage", e)
        _disable()
    except OSError as e:
        LOG.warning("sqlite init OS error (%s); disabling storage", e)
        _disable()


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def write_snapshot(rows: list[dict], ts: int | None = None) -> None:
    """Persist one snapshot row per ticker. `rows` is the aggregator output."""
    if not rows:
        return
    ts = ts or int(time.time())
    with connect() as c:
        if c is None:
            return
        c.executemany(
            "INSERT INTO snapshots(ts, ticker, mentions, bullish, bearish, "
            "neutral, avg_sentiment, price, change_pct, sources_json) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            [(ts, r["symbol"], r["mentions"], r["bullish"], r["bearish"],
              r["neutral"], r["avg_sentiment"], r.get("price"),
              r.get("change_pct"), json.dumps(r.get("sources") or {}))
             for r in rows],
        )


def history(ticker: str, since_ts: int) -> list[dict]:
    with connect() as c:
        if c is None:
            return []
        cur = c.execute(
            "SELECT ts, mentions, bullish, bearish, neutral, avg_sentiment, "
            "price, change_pct FROM snapshots "
            "WHERE ticker=? AND ts>=? ORDER BY ts ASC",
            (ticker, since_ts),
        )
        return [dict(r) for r in cur.fetchall()]


def prior_mentions(ticker: str, ts: int, lookback_sec: int) -> int:
    """Mentions in the snapshot closest to (ts - lookback_sec). 0 if none."""
    target = ts - lookback_sec
    with connect() as c:
        if c is None:
            return 0
        cur = c.execute(
            "SELECT mentions FROM snapshots WHERE ticker=? "
            "ORDER BY ABS(ts - ?) ASC LIMIT 1",
            (ticker, target),
        )
        row = cur.fetchone()
        return int(row["mentions"]) if row else 0


def prior_sentiment(ticker: str, ts: int, lookback_sec: int) -> float | None:
    target = ts - lookback_sec
    with connect() as c:
        if c is None:
            return None
        cur = c.execute(
            "SELECT avg_sentiment FROM snapshots WHERE ticker=? AND ts<=? "
            "ORDER BY ts DESC LIMIT 1",
            (ticker, target),
        )
        row = cur.fetchone()
        return float(row["avg_sentiment"]) if row else None


def baseline_mentions(ticker: str, ts: int, days: int = 14) -> tuple[float, float]:
    """Return (mean, stdev) of mentions over the trailing `days`. (0,0) if empty."""
    since = ts - days * 86400
    with connect() as c:
        if c is None:
            return 0.0, 0.0
        cur = c.execute(
            "SELECT mentions FROM snapshots WHERE ticker=? AND ts BETWEEN ? AND ?",
            (ticker, since, ts - 1),
        )
        vals = [int(r["mentions"]) for r in cur.fetchall()]
    if len(vals) < 3:
        return 0.0, 0.0
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    return mean, var ** 0.5


def sparkline(ticker: str, ts: int, hours: int = 24, points: int = 24) -> list[int]:
    """Return up to `points` mention counts over the trailing `hours`,
    bucketed evenly. Older buckets first, missing buckets = 0."""
    since = ts - hours * 3600
    bucket_sec = max(1, (hours * 3600) // points)
    out = [0] * points
    with connect() as c:
        if c is None:
            return out
        cur = c.execute(
            "SELECT ts, mentions FROM snapshots "
            "WHERE ticker=? AND ts BETWEEN ? AND ? ORDER BY ts ASC",
            (ticker, since, ts),
        )
        for r in cur.fetchall():
            idx = min(points - 1, int((r["ts"] - since) // bucket_sec))
            out[idx] = max(out[idx], int(r["mentions"]))
    return out


def prune_snapshots(older_than_days: int = 30) -> int:
    cutoff = int(time.time()) - older_than_days * 86400
    with connect() as c:
        if c is None:
            return 0
        cur = c.execute("DELETE FROM snapshots WHERE ts < ?", (cutoff,))
        return cur.rowcount


# ---------------------------------------------------------------------------
# Messages (drill-down)
# ---------------------------------------------------------------------------

def write_messages(messages: Iterable[dict], ts: int | None = None) -> None:
    ts = ts or int(time.time())
    with connect() as c:
        if c is None:
            return
        c.executemany(
            "INSERT INTO messages(ts, ticker, source, text, score, url, sentiment) "
            "VALUES(?,?,?,?,?,?,?)",
            [(ts, m["ticker"], m.get("source"), (m.get("text") or "")[:1000],
              int(m.get("score") or 0), m.get("url"), float(m.get("sentiment") or 0.0))
             for m in messages],
        )


def recent_messages(ticker: str, limit: int = 50) -> list[dict]:
    with connect() as c:
        if c is None:
            return []
        cur = c.execute(
            "SELECT ts, source, text, score, url, sentiment FROM messages "
            "WHERE ticker=? ORDER BY ts DESC, id DESC LIMIT ?",
            (ticker, limit),
        )
        return [dict(r) for r in cur.fetchall()]


def prune_messages(older_than_days: int = 7) -> int:
    cutoff = int(time.time()) - older_than_days * 86400
    with connect() as c:
        if c is None:
            return 0
        cur = c.execute("DELETE FROM messages WHERE ts < ?", (cutoff,))
        return cur.rowcount


# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------

def watchlist_get() -> list[str]:
    with connect() as c:
        if c is None:
            return []
        return [r["symbol"] for r in
                c.execute("SELECT symbol FROM watchlist ORDER BY symbol")]


def watchlist_add(symbol: str) -> None:
    with connect() as c:
        if c is None:
            return
        c.execute("INSERT OR IGNORE INTO watchlist(symbol, added_at) VALUES(?, ?)",
                  (symbol.upper(), int(time.time())))


def watchlist_remove(symbol: str) -> None:
    with connect() as c:
        if c is None:
            return
        c.execute("DELETE FROM watchlist WHERE symbol=?", (symbol.upper(),))


# ---------------------------------------------------------------------------
# Alerts dedupe log
# ---------------------------------------------------------------------------

def alert_already_fired(ticker: str, kind: str, within_sec: int) -> bool:
    cutoff = int(time.time()) - within_sec
    with connect() as c:
        if c is None:
            # Without dedupe storage, treat as "already fired" so we don't
            # spam every refresh on stateless hosts.
            return True
        cur = c.execute(
            "SELECT 1 FROM alerts_log WHERE ticker=? AND kind=? AND ts>=? LIMIT 1",
            (ticker, kind, cutoff),
        )
        return cur.fetchone() is not None


def record_alert(ticker: str, kind: str, payload: dict) -> None:
    with connect() as c:
        if c is None:
            return
        c.execute(
            "INSERT INTO alerts_log(ts, ticker, kind, payload_json) VALUES(?,?,?,?)",
            (int(time.time()), ticker, kind, json.dumps(payload)),
        )


init()

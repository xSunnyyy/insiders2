"""Alert engine.

Rules evaluated against each refresh's ranked rows + their deltas:

  - new_top5         : ticker entered top 5 by mentions for the first time today
  - mention_spike    : mentions are >=3sigma above the trailing baseline
  - sentiment_flip   : avg_sentiment crossed +/-0.3 vs ~24h ago
  - watchlist_chatter: any watchlist ticker has at least N mentions

Channels (any combination, configured via env vars):

  DISCORD_WEBHOOK    -- full URL of a Discord channel webhook
  SLACK_WEBHOOK      -- full URL of a Slack incoming webhook
  TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
  ALERT_WEBHOOK      -- generic POST target; receives JSON payload as-is

Alerts are deduped per-ticker per-rule for 6 hours so we don't spam.
"""

from __future__ import annotations

import logging
import os

import requests

import db

LOG = logging.getLogger(__name__)

DEDUPE_SEC = 6 * 3600
SPIKE_SIGMA = 3.0
SPIKE_MIN_MENTIONS = 10           # ignore noise on tiny baselines
FLIP_DELTA = 0.3
WATCHLIST_MIN_MENTIONS = 3


# ---------------------------------------------------------------------------
# Channel senders
# ---------------------------------------------------------------------------

def _post(url: str, payload: dict, timeout: float = 6.0) -> bool:
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code >= 300:
            LOG.warning("alert post %s -> %s: %s", url[:50], r.status_code,
                        r.text[:200])
            return False
        return True
    except requests.RequestException as e:
        LOG.warning("alert post failed: %s", e)
        return False


def _send_discord(text: str) -> None:
    url = os.environ.get("DISCORD_WEBHOOK")
    if url:
        _post(url, {"content": text})


def _send_slack(text: str) -> None:
    url = os.environ.get("SLACK_WEBHOOK")
    if url:
        _post(url, {"text": text})


def _send_telegram(text: str) -> None:
    tok = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat = os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            json={"chat_id": chat, "text": text,
                  "disable_web_page_preview": True},
            timeout=6,
        )
    except requests.RequestException as e:
        LOG.warning("telegram send failed: %s", e)


def _send_generic(payload: dict) -> None:
    url = os.environ.get("ALERT_WEBHOOK")
    if url:
        _post(url, payload)


def _broadcast(kind: str, ticker: str, text: str, payload: dict) -> None:
    LOG.info("ALERT [%s] %s -- %s", kind, ticker, text)
    _send_discord(text)
    _send_slack(text)
    _send_telegram(text)
    _send_generic({"kind": kind, "ticker": ticker, "text": text, **payload})


# ---------------------------------------------------------------------------
# Rule engine
# ---------------------------------------------------------------------------

def _emit(kind: str, ticker: str, text: str, payload: dict | None = None) -> None:
    if db.alert_already_fired(ticker, kind, DEDUPE_SEC):
        return
    payload = payload or {}
    db.record_alert(ticker, kind, payload)
    _broadcast(kind, ticker, text, payload)


def evaluate(rows: list[dict], ts: int) -> None:
    """Run all rules against this snapshot. `rows` is the aggregator output
    after deltas/baselines have been merged in."""
    watchlist = set(db.watchlist_get())

    # Determine current top-5 by mentions.
    top5 = {r["symbol"] for r in rows[:5]}

    for r in rows:
        sym = r["symbol"]
        mentions = int(r.get("mentions") or 0)
        sent = float(r.get("avg_sentiment") or 0.0)
        prev_sent = r.get("prior_sentiment_24h")
        baseline = r.get("baseline_mean") or 0.0
        sigma = r.get("baseline_std") or 0.0

        # Rule: new in top 5
        if sym in top5:
            _emit("new_top5", sym,
                  f"[top5] {sym} entered top 5 with {mentions} mentions, "
                  f"sentiment {sent:+.2f} ({r.get('trend','?')})",
                  {"mentions": mentions, "sentiment": sent,
                   "rank": [x['symbol'] for x in rows[:5]].index(sym) + 1})

        # Rule: mention spike (z-score)
        if (mentions >= SPIKE_MIN_MENTIONS and sigma > 0
                and (mentions - baseline) / sigma >= SPIKE_SIGMA):
            z = (mentions - baseline) / sigma
            _emit("mention_spike", sym,
                  f"[spike] {sym} mentions={mentions} "
                  f"(baseline {baseline:.1f}, z={z:.1f}), sentiment {sent:+.2f}",
                  {"mentions": mentions, "z_score": z,
                   "baseline": baseline, "sentiment": sent})

        # Rule: sentiment flip vs ~24h ago
        if prev_sent is not None and abs(sent - prev_sent) >= FLIP_DELTA \
                and (sent > 0) != (prev_sent > 0):
            _emit("sentiment_flip", sym,
                  f"[flip] {sym} sentiment {prev_sent:+.2f} -> {sent:+.2f} "
                  f"(mentions {mentions})",
                  {"sentiment": sent, "prev_sentiment": prev_sent,
                   "mentions": mentions})

        # Rule: watchlist chatter
        if sym in watchlist and mentions >= WATCHLIST_MIN_MENTIONS:
            _emit("watchlist_chatter", sym,
                  f"[watch] {sym} has {mentions} mentions, "
                  f"sentiment {sent:+.2f} ({r.get('trend','?')})",
                  {"mentions": mentions, "sentiment": sent})


def channels_configured() -> list[str]:
    out: list[str] = []
    if os.environ.get("DISCORD_WEBHOOK"):    out.append("discord")
    if os.environ.get("SLACK_WEBHOOK"):      out.append("slack")
    if os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"):
        out.append("telegram")
    if os.environ.get("ALERT_WEBHOOK"):      out.append("webhook")
    return out

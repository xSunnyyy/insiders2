"""Sentiment scoring.

Backends, in preference order, controlled by SENTIMENT_BACKEND env var:

  - 'finbert' : ProsusAI/finbert via transformers (heavy, ~400MB, finance-tuned).
                Used if installed and explicitly requested.
  - 'vader'   : vaderSentiment with our finance lexicon merged in. Default.
  - 'lexicon' : the original hand-rolled bullish/bearish word list (no deps).

Public API stays the same:
    score(text) -> float in [-1, 1]    positive = bullish
    label(s)    -> 'bullish' | 'bearish' | 'neutral'
"""

from __future__ import annotations

import os
import re

# ---------------------------------------------------------------------------
# Hand-rolled finance lexicon (used by both the lexicon backend and to
# augment VADER, which doesn't know "printing"/"bag holder"/etc).
# ---------------------------------------------------------------------------

BULLISH = {
    "bull", "bullish", "buy", "buying", "bought", "long", "longs", "calls",
    "call", "moon", "mooning", "rocket", "🚀", "rip", "ripping", "pump",
    "pumping", "squeeze", "squeezing", "rally", "rallying", "breakout",
    "breaking", "surge", "surging", "soar", "soaring",
    "uptrend", "up", "green", "gains", "gain", "winning", "winner", "tendies",
    "diamond", "hodl", "hold", "holding", "accumulate", "accumulating",
    "undervalued", "oversold", "support", "bottom", "bottomed", "reversal",
    "beat", "beats", "beating", "strong", "outperform", "upgrade", "upgraded",
    "raise", "raised", "raises", "guidance", "record", "ath", "all-time",
    "printing", "prints", "ripper", "ripped",
    "🌙", "💎", "🙌", "📈", "🟢",
}

BEARISH = {
    "bear", "bearish", "sell", "selling", "sold", "short", "shorts", "shorting",
    "puts", "put", "crash", "crashing", "dump", "dumping", "drop", "dropping",
    "fall", "falling", "fell", "tank", "tanking", "plunge", "plunging",
    "downtrend", "down", "red", "loss", "losses", "losing", "loser", "bagholder",
    "bagholders", "rugpull", "rug", "scam", "fraud", "fud", "weak", "weakness",
    "underperform", "downgrade", "downgraded", "cut", "cuts", "miss", "missed",
    "missing", "warning", "warned", "concern", "concerns", "risk", "risky",
    "overvalued", "overbought", "resistance", "top", "topped", "rejection",
    "rejected", "bankrupt", "bankruptcy", "delist", "delisted", "halt",
    "halted", "subpoena", "investigation", "lawsuit", "sue", "sued",
    "📉", "🔴", "🩸", "💀",
}

WORD_RE = re.compile(r"[A-Za-z']+|🚀|🌙|💎|🙌|📈|🟢|📉|🔴|🩸|💀")
NEGATORS = {"not", "no", "never", "without", "isn't", "wasn't", "won't",
            "don't", "doesn't", "didn't", "ain't", "can't"}


# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------

_REQUESTED = os.environ.get("SENTIMENT_BACKEND", "vader").lower()
_BACKEND = "lexicon"
_vader = None
_finbert = None


def _try_finbert():
    """Lazy-load FinBERT. Returns a callable(text) -> float or None on failure."""
    try:
        from transformers import (AutoModelForSequenceClassification,
                                  AutoTokenizer)
        import torch
    except ImportError:
        return None
    try:
        tok = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        mdl = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        labels = ["positive", "negative", "neutral"]

        def _fn(text: str) -> float:
            if not text:
                return 0.0
            inp = tok(text[:512], return_tensors="pt", truncation=True)
            with torch.no_grad():
                logits = mdl(**inp).logits[0]
            probs = torch.softmax(logits, dim=-1).tolist()
            d = dict(zip(labels, probs))
            return float(d["positive"] - d["negative"])

        return _fn
    except Exception:
        return None


def _try_vader():
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    except ImportError:
        return None
    a = SentimentIntensityAnalyzer()
    # Augment VADER's lexicon with our finance terms.
    finance_lex = {}
    for w in BULLISH:
        finance_lex[w.lower()] = 2.0
    for w in BEARISH:
        finance_lex[w.lower()] = -2.0
    a.lexicon.update(finance_lex)
    return a


if _REQUESTED == "finbert":
    _finbert = _try_finbert()
    if _finbert:
        _BACKEND = "finbert"

if _BACKEND == "lexicon":  # finbert not chosen / failed
    _vader = _try_vader()
    if _vader is not None:
        _BACKEND = "vader"


def backend() -> str:
    return _BACKEND


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_lexicon(text: str) -> float:
    if not text:
        return 0.0
    tokens = [t.lower() for t in WORD_RE.findall(text)]
    if not tokens:
        return 0.0
    bull = bear = 0
    for i, tok in enumerate(tokens):
        negated = i > 0 and tokens[i - 1] in NEGATORS
        if tok in BULLISH:
            bear += 1 if negated else 0
            bull += 0 if negated else 1
        elif tok in BEARISH:
            bull += 1 if negated else 0
            bear += 0 if negated else 1
    total = bull + bear
    if total == 0:
        return 0.0
    return (bull - bear) / total


def score(text: str) -> float:
    if not text:
        return 0.0
    if _BACKEND == "finbert" and _finbert is not None:
        try:
            return float(_finbert(text))
        except Exception:
            pass
    if _BACKEND == "vader" and _vader is not None:
        return float(_vader.polarity_scores(text)["compound"])
    return _score_lexicon(text)


def label(s: float) -> str:
    if s >= 0.15:
        return "bullish"
    if s <= -0.15:
        return "bearish"
    return "neutral"

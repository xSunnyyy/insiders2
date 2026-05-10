"""Lexicon-based bullish/bearish sentiment scoring.

Returns a score in [-1.0, 1.0]: positive = bullish, negative = bearish.
Designed to be cheap and dependency-free. For inputs that already carry
explicit sentiment (Stocktwits' `entities.sentiment.basic`), use that
directly instead of running this scorer.
"""

import re

BULLISH = {
    "bull", "bullish", "buy", "buying", "bought", "long", "longs", "calls",
    "call", "moon", "mooning", "rocket", "🚀", "rip", "ripping", "pump",
    "pumping", "squeeze", "squeezing", "rally", "rallying", "breakout",
    "breaking", "surge", "surging", "soar", "soaring", "rip", "rip",
    "uptrend", "up", "green", "gains", "gain", "winning", "winner", "tendies",
    "diamond", "hodl", "hold", "holding", "accumulate", "accumulating",
    "undervalued", "oversold", "support", "bottom", "bottomed", "reversal",
    "beat", "beats", "beating", "strong", "outperform", "upgrade", "upgraded",
    "raise", "raised", "raises", "guidance", "record", "ath", "all-time",
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


def score(text: str) -> float:
    """Return sentiment in [-1, 1] for `text`. Neutral/empty -> 0.0."""
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


def label(s: float) -> str:
    if s >= 0.15:
        return "bullish"
    if s <= -0.15:
        return "bearish"
    return "neutral"

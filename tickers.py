"""Ticker extraction and a curated allowlist to filter false positives.

The allowlist is a pragmatic snapshot of high-volume / commonly-discussed US
tickers. Anything not on the list is still accepted if prefixed with `$`.
"""

import re

# Common 1-5 letter all-caps English words that look like tickers but aren't.
STOPWORDS = {
    "A", "I", "AN", "AS", "AT", "BE", "BY", "DO", "GO", "HE", "IF", "IN", "IS",
    "IT", "ME", "MY", "NO", "OF", "ON", "OR", "SO", "TO", "UP", "US", "WE",
    "ALL", "AND", "ANY", "ARE", "BUT", "CAN", "DID", "FOR", "GET", "GOT", "HAD",
    "HAS", "HER", "HIM", "HIS", "HOW", "ITS", "LET", "MAY", "NEW", "NOT", "NOW",
    "OLD", "ONE", "OUR", "OUT", "OWN", "PUT", "SAY", "SEE", "SHE", "THE", "TOO",
    "TOP", "TRY", "TWO", "USE", "WAS", "WAY", "WHO", "WHY", "YES", "YET", "YOU",
    "ABLE", "ALSO", "BACK", "BEEN", "BEST", "BOTH", "CALL", "CAME", "COME",
    "DOES", "DONE", "DOWN", "EACH", "ELSE", "EVEN", "EVER", "FELT", "FROM",
    "GIVE", "GOES", "GONE", "GOOD", "HAVE", "HEAR", "HERE", "HIGH", "HOLD",
    "INTO", "JUST", "KEEP", "KIND", "KNEW", "KNOW", "LAST", "LEFT", "LESS",
    "LIFE", "LIKE", "LIVE", "LONG", "LOOK", "MADE", "MAKE", "MANY", "MORE",
    "MOST", "MUCH", "MUST", "NEED", "NEXT", "ONLY", "OPEN", "OVER", "PART",
    "PAST", "PLAN", "PLAY", "READ", "REAL", "RIGHT", "SAID", "SAME", "SEEM",
    "SEEN", "SHOW", "SIDE", "SOME", "SOON", "STOP", "SUCH", "SURE", "TAKE",
    "TELL", "THAN", "THAT", "THEM", "THEN", "THEY", "THIS", "THUS", "TIME",
    "TOLD", "TOOK", "TURN", "VERY", "WANT", "WELL", "WENT", "WERE", "WHAT",
    "WHEN", "WILL", "WITH", "WORK", "YEAR", "YOUR", "ABOUT", "AFTER", "AGAIN",
    "ASKED", "BEING", "BELOW", "COULD", "EARLY", "EVERY", "FOUND", "GIVEN",
    "GOING", "GREAT", "GROUP", "HEARD", "HOUSE", "LARGE", "LATER", "LEAST",
    "LEAVE", "LIVED", "MIGHT", "NEVER", "OFTEN", "ORDER", "OTHER", "PLACE",
    "RIGHT", "SHALL", "SMALL", "STILL", "TAKEN", "THEIR", "THERE", "THESE",
    "THING", "THINK", "THOSE", "THREE", "TODAY", "UNDER", "UNTIL", "USING",
    "WHERE", "WHICH", "WHILE", "WHOLE", "WORLD", "WOULD", "YEARS", "YOUNG",
    "BEFORE", "BEHIND", "BETTER", "BEYOND", "CHANGE", "DURING", "ENOUGH",
    "FRIEND", "GROUND", "HAVING", "ITSELF", "LITTLE", "MAKING", "MOMENT",
    "MYSELF", "NEEDED", "NOTHING", "OFFICE", "PEOPLE", "PERSON", "PLEASE",
    "RATHER", "REALLY", "REASON", "SAYING", "SECOND", "SHOULD", "SIMPLE",
    "STREET", "TAKING", "THINGS", "THOUGH", "TRYING", "TURNED", "WANTED",
    "WITHIN", "ALWAYS", "AROUND", "BEHIND",
    # Reddit / finance argot that masquerades as tickers
    "DD", "IPO", "ATH", "ATL", "EPS", "PE", "PEG", "ETF", "CEO", "CFO", "CTO",
    "COO", "USD", "USA", "EU", "UK", "UN", "FED", "FOMC", "SEC", "FINRA", "IRS",
    "GDP", "CPI", "PPI", "FUD", "WSB", "YOLO", "FOMO", "MOON", "BULL", "BEAR",
    "LONG", "SHORT", "PUTS", "CALLS", "OTM", "ITM", "ATM", "DTE", "RH", "TDA",
    "FYI", "LOL", "LMAO", "TLDR", "TLDR", "OP", "PSA", "AMA", "EOD", "EOW",
    "BTW", "IMO", "IMHO", "AFAIK", "LMK", "TBH", "TYIA", "NGL", "FWIW", "OG",
    "GG", "WP", "GL", "HF", "RIP", "NSFW", "AF",
}

# A snapshot of widely-traded US tickers. Not exhaustive; new mentions are
# also captured via the `$TICKER` cashtag pattern.
KNOWN_TICKERS = {
    "AAPL", "MSFT", "GOOG", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "AMD",
    "INTC", "NFLX", "DIS", "BA", "JPM", "BAC", "WFC", "GS", "MS", "C", "V",
    "MA", "PYPL", "SQ", "SHOP", "COIN", "HOOD", "SOFI", "PLTR", "RBLX", "U",
    "SNAP", "PINS", "TWTR", "X", "UBER", "LYFT", "ABNB", "DASH", "ROKU",
    "SPOT", "ZM", "DOCU", "CRWD", "NET", "DDOG", "SNOW", "MDB", "OKTA",
    "ZS", "PANW", "FTNT", "TEAM", "WDAY", "NOW", "ADBE", "CRM", "ORCL",
    "IBM", "CSCO", "QCOM", "AVGO", "TXN", "MU", "AMAT", "LRCX", "KLAC",
    "ASML", "TSM", "ARM", "SMCI", "MRVL", "ON", "NXPI", "STX", "WDC",
    "T", "VZ", "TMUS", "CMCSA", "CHTR", "WBD", "PARA", "F", "GM", "STLA",
    "RIVN", "LCID", "NIO", "XPEV", "LI", "TM", "HMC", "FORD",
    "WMT", "TGT", "COST", "HD", "LOW", "BBY", "DG", "DLTR", "KR", "WBA",
    "CVS", "UNH", "JNJ", "PFE", "MRK", "ABBV", "LLY", "BMY", "GILD", "AMGN",
    "MRNA", "BNTX", "NVAX", "REGN", "VRTX", "BIIB", "ZTS", "CI", "HUM",
    "XOM", "CVX", "BP", "SHEL", "COP", "OXY", "MPC", "VLO", "PSX", "SLB",
    "HAL", "EOG", "PXD", "DVN", "FANG",
    "GME", "AMC", "BB", "BBBY", "NOK", "SNDL", "TLRY", "CGC", "ACB",
    "SPY", "QQQ", "DIA", "IWM", "VTI", "VOO", "VEA", "VWO", "BND", "AGG",
    "GLD", "SLV", "USO", "UNG", "TLT", "TQQQ", "SQQQ", "SOXL", "SOXS",
    "TNA", "TZA", "SPXL", "SPXS", "UVXY", "VIX", "VXX", "ARKK", "ARKG",
    "ARKW", "ARKQ", "ARKF", "XLF", "XLK", "XLE", "XLV", "XLI", "XLP",
    "XLY", "XLU", "XLB", "XLRE", "XLC", "SMH", "SOXX", "KRE", "XBI",
    "JETS", "ITA", "ITB", "XHB", "GDX", "GDXJ", "URNM", "URA", "LIT",
    "BTC", "ETH", "MSTR", "MARA", "RIOT", "CLSK", "HUT", "BITF", "BITO",
    "BRK.A", "BRK.B", "BRKB", "PG", "KO", "PEP", "MCD", "SBUX", "CMG",
    "YUM", "DPZ", "QSR", "WEN", "PZZA", "NKE", "LULU", "RL", "TPR", "PVH",
    "GAP", "ANF", "URBN", "AEO", "EL", "ULTA", "LVMUY",
    "SPOT", "DKNG", "PENN", "MGM", "WYNN", "LVS", "CZR", "BYD",
    "PLUG", "FCEL", "BLDP", "BE", "ENPH", "SEDG", "RUN", "SPWR", "FSLR",
    "NEE", "DUK", "SO", "AEP", "EXC", "D", "PCG", "ED", "PEG",
    "BABA", "JD", "PDD", "BIDU", "NTES", "TCEHY", "TME", "BILI", "VIPS",
    "TAL", "EDU", "IQ", "DIDI", "BEKE",
}


CASHTAG_RE = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z])?)\b")
BARETAG_RE = re.compile(r"\b([A-Z]{1,5})\b")


def extract_tickers(text: str) -> set[str]:
    """Return the set of unique tickers found in `text`."""
    if not text:
        return set()
    found: set[str] = set()
    for m in CASHTAG_RE.finditer(text):
        found.add(m.group(1).upper())
    for m in BARETAG_RE.finditer(text):
        sym = m.group(1).upper()
        if sym in STOPWORDS:
            continue
        if sym in KNOWN_TICKERS:
            found.add(sym)
    return found

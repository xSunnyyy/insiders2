# Social Stock Sentiment

A small Flask web app that scrapes social media for stock chatter and shows the
top-20 most-mentioned tickers with a bullish / bearish / neutral trend.

## Sources

| Platform     | Method                                                                           | Status |
|--------------|----------------------------------------------------------------------------------|--------|
| Reddit       | OAuth API on `wallstreetbets`, `stocks`, `investing`, `StockMarket`, `options`, `pennystocks`, `Daytrading`, `Superstonk`, `stock_picks`. Falls back to public `.json`, then to **Pullpush.io** (Pushshift mirror, no auth needed). | works without a key |
| Stocktwits   | Public `api.stocktwits.com/api/2` trending + per-symbol streams                  | works (no key) |
| Bluesky      | Public AppView `app.bsky.feed.searchPosts` (no auth)                             | works (no key) |
| Twitter / X  | v2 `recent search` endpoint (paid). Set `TWITTER_BEARER_TOKEN` to enable.        | opt-in |

> Twitter no longer permits free anonymous scraping (`snscrape` and the
> guest-token flows have been broken since 2023). Without a paid bearer token
> the Twitter source silently returns nothing — Reddit + Stocktwits still
> produce a useful ranking on their own.

## Reddit OAuth (strongly recommended)

Since the 2023 API changes, Reddit aggressively blocks unauthenticated
requests from datacenter IPs and generic UAs. If your dashboard shows
`reddit(0; 403 anon)` or similar, you need OAuth credentials. They're
**free**:

1. Visit https://www.reddit.com/prefs/apps → **create another app**
2. Pick type **script**. Set redirect URI to `http://localhost:8080`
3. Copy the **client ID** (under the app name) and the **secret**

Then export before running:

```bash
export REDDIT_CLIENT_ID=...
export REDDIT_CLIENT_SECRET=...
export REDDIT_USER_AGENT="linux:stock-sentiment:0.1 (by /u/yourname)"
```

The scraper transparently switches to `oauth.reddit.com` when these are
set. App-only tokens give you 100 requests/min, plenty for this workload.

## Live quotes

Stocktwits' free messaging API doesn't return prices, so the dashboard pulls
**price + change %** for the top-20 tickers from Yahoo Finance's
unauthenticated v8 chart endpoint (`prices.py`). Quotes are fetched in
parallel and cached alongside the rest of the snapshot (5-min TTL).

## How sentiment is decided

- **Stocktwits** messages often carry an explicit `Bullish` / `Bearish` tag the
  poster chose. We use that directly when present.
- Everything else is scored with **VADER** (`vaderSentiment`) augmented with
  a finance lexicon (so terms like "printing", "bag holder", "rip", "calls",
  "puts" get the right polarity). VADER handles negation/intensifiers/emoji
  correctly. Falls back to a hand-rolled lexicon if VADER isn't installed.
- Optional: set `SENTIMENT_BACKEND=finbert` and install `transformers` +
  `torch` to use FinBERT (ProsusAI/finbert) -- materially more accurate on
  finance text but ~400 MB and slower.
- Per-ticker scores are engagement-weighted so a viral post counts more
  than a one-off comment.

A ticker is labeled **bullish** when its weighted average score >= +0.15,
**bearish** when <= -0.15, otherwise **neutral**.

## History, deltas, and catalysts

Every refresh writes a snapshot to a local SQLite file (default
`data.sqlite3`, override with `DB_PATH`). From that history the dashboard
shows:

- **Δ1h / Δ24h mentions** -- how much chatter grew vs ~1h and ~24h ago.
- **z-score** of current mentions vs the trailing 14-day baseline. >= 3σ
  triggers a spike alert.
- **Sparkline** of mentions over the last 24 hours.
- **ΔSent 24h** -- sentiment delta vs ~24h ago, useful for spotting flips.
- **Catalyst** column -- nearest upcoming earnings date and the most recent
  news headline (Yahoo Finance, free).
- **Drill-down page** at `/ticker/<sym>` with mention/sentiment/price
  charts, recent messages from each source, and news.

Snapshot rows older than 30 days and message bodies older than 7 days are
pruned automatically.

## Watchlist

Click ★ next to a ticker to pin it. Watchlisted tickers are stored in
SQLite (`watchlist` table) and any chatter on them above a small threshold
fires an alert (`watchlist_chatter`) regardless of where they rank.

## Alerts

Configure any combination of the following env vars to start receiving
push alerts when rules trigger:

```bash
export DISCORD_WEBHOOK="https://discord.com/api/webhooks/..."
export SLACK_WEBHOOK="https://hooks.slack.com/services/..."
export TELEGRAM_BOT_TOKEN="..." TELEGRAM_CHAT_ID="..."
export ALERT_WEBHOOK="https://example.com/your-endpoint"   # generic JSON POST
```

Rules (deduped per-ticker per-rule for 6 hours):

- `new_top5`          -- ticker entered the top 5 by mentions
- `mention_spike`     -- mentions >= 3σ above trailing baseline (and >= 10)
- `sentiment_flip`    -- avg_sentiment crossed +/-0.3 vs ~24h ago
- `watchlist_chatter` -- a watchlist ticker has at least 3 mentions

## Running

```bash
pip install -r requirements.txt
python app.py            # then open http://localhost:5000
```

Optional:

```bash
export TWITTER_BEARER_TOKEN=...   # enables the Twitter source
```

## API

- `GET  /api/trending`           cached top-20 JSON (5-min TTL)
- `POST /api/refresh`            force a re-scrape
- `GET  /api/ticker/<sym>`       drill-down JSON (history + messages + catalysts)
- `GET  /api/watchlist`          list watchlist
- `POST /api/watchlist`          add `{"symbol": "AAPL"}`
- `DELETE /api/watchlist/<sym>`  remove from watchlist

## Caveats

- Public endpoints can rate-limit or change; expect occasional empty source
  blocks. The UI shows which sources contributed and how many items each
  produced.
- This is a sentiment *snapshot*, not financial advice.

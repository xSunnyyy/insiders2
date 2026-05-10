# Social Stock Sentiment

A small Flask web app that scrapes social media for stock chatter and shows the
top-20 most-mentioned tickers with a bullish / bearish / neutral trend.

## Sources

| Platform     | Method                                                                           | Status |
|--------------|----------------------------------------------------------------------------------|--------|
| Reddit       | Public `.json` endpoints on `wallstreetbets`, `stocks`, `investing`, `StockMarket`, `options`, `pennystocks`, `Daytrading`, `Superstonk`, `stock_picks` | works (no key) |
| Stocktwits   | Public `api.stocktwits.com/api/2` trending + per-symbol streams                  | works (no key) |
| Bluesky      | Public AppView `app.bsky.feed.searchPosts` (no auth)                             | works (no key) |
| Twitter / X  | v2 `recent search` endpoint (paid). Set `TWITTER_BEARER_TOKEN` to enable.        | opt-in |

> Twitter no longer permits free anonymous scraping (`snscrape` and the
> guest-token flows have been broken since 2023). Without a paid bearer token
> the Twitter source silently returns nothing — Reddit + Stocktwits still
> produce a useful ranking on their own.

## How sentiment is decided

- **Stocktwits** messages often carry an explicit `Bullish` / `Bearish` tag the
  poster chose. We use that directly when present.
- Everything else is scored with a small bullish/bearish lexicon
  (`sentiment.py`), with single-word negation handling (`not bullish` flips).
- Per-ticker scores are upvote-weighted so a viral post counts more than a
  one-off comment.

A ticker is labeled **bullish** when its weighted average score >= +0.15,
**bearish** when <= -0.15, otherwise **neutral**.

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

- `GET /api/trending` - cached top-20 JSON (5-min TTL)
- `POST /api/refresh` - force a re-scrape

## Caveats

- Public endpoints can rate-limit or change; expect occasional empty source
  blocks. The UI shows which sources contributed and how many items each
  produced.
- This is a sentiment *snapshot*, not financial advice.

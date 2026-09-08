# Crypto Metrics Dashboard

A single-file Flask app that compares every USDT-margined perpetual on Binance
Futures across four metrics — **daily return**, **standard deviation of daily
returns**, **open interest**, and **implied volatility** — over a lookback
window you choose, with sorting and filtering on every column.

No build step, no database, no API keys. One file (`app.py`), Tailwind and
Chart.js from CDN. The interface itself is in Thai.

## Features

- **Pick a window** — 7, 30, 90, 180 days or 1 year. Every metric recomputes.
- **Sort by anything** — click a column header (click again to flip direction),
  or use the dropdown. Rank by highest daily return over the past year, lowest
  volatility, largest open interest, and so on.
- **Stack filters** — build conditions such as `open interest ≥ $100M` plus
  `daily SD ≤ 4%`, then sort the survivors by IV. Each condition shows as a chip
  you can remove individually.
- **Per-coin charts** — click any row for daily close price, daily returns with
  a 14-day rolling standard deviation overlay, open interest, and implied vs.
  realized volatility.
- **Preset views** — highest daily return, low volatility with positive returns,
  largest open interest, highest IV.
- **Progressive loading** — the table fills in as data arrives instead of
  blocking on a slow first request.
- **Demo mode** — generates deterministic sample data when the exchange APIs are
  unreachable, so the UI is always explorable.

## Requirements

- Python 3.9 or newer
- `flask` and `requests`

## Installation

```bash
pip install flask requests
```

## Usage

```bash
python app.py
```

Then open <http://127.0.0.1:5000>.

On Windows PowerShell 5.1, run the two commands on separate lines — `&&` is not
a valid separator in that shell.

The first load fetches around 80 symbols and takes a few seconds. Results are
cached for 5 minutes; the refresh button in the header forces a re-fetch.

## Configuration

Set environment variables before starting the app.

| Variable | Default | What it does |
| --- | --- | --- |
| `PORT` | `5000` | Port to listen on |
| `HOST` | `127.0.0.1` | Bind address |
| `UNIVERSE_SIZE` | `80` | How many symbols to track, ranked by 24h quote volume |
| `CACHE_TTL` | `300` | Seconds to cache exchange responses |
| `REQUEST_DEADLINE` | `8` | Max seconds one API request may take before returning partial results |
| `HTTP_TIMEOUT` | `10` | Read timeout for a single exchange call |
| `MAX_WORKERS` | `12` | Concurrent fetch threads |
| `DEMO` | `0` | Set to `1` to force generated demo data |

Examples:

```bash
PORT=8080 UNIVERSE_SIZE=30 python app.py
```

```powershell
$env:PORT="8080"; $env:UNIVERSE_SIZE="30"; python app.py
```

## HTTP API

The page is driven by these endpoints, which you can also call directly.

| Endpoint | Description |
| --- | --- |
| `GET /data/coins` | Summary metrics for every tracked symbol |
| `GET /data/coin/<symbol>` | Full daily series for one symbol |
| `GET /data/health` | Data source status |

`/api/metrics`, `/api/coin/<symbol>` and `/api/health` are kept as aliases, but
prefer the `/data/*` paths — some ad blockers match `/api/metrics` against their
analytics filter lists and cancel the request before it reaches the server.

**Query parameters for `/data/coins`:**

| Parameter | Example | Meaning |
| --- | --- | --- |
| `days` | `365` | Lookback window: 7, 30, 90, 180 or 365 |
| `sort` | `iv` | Field to sort by |
| `order` | `asc` | `asc` or `desc` (default) |
| `q` | `BTC` | Substring match on the symbol |
| `min_<field>` | `min_oi_usd=100000000` | Keep rows at or above this value |
| `max_<field>` | `max_std_daily=4` | Keep rows at or below this value |
| `refresh` | `1` | Bypass the cache |

```bash
curl "http://127.0.0.1:5000/data/coins?days=365&sort=iv&order=desc&min_oi_usd=100000000"
```

Sortable and filterable fields: `total_return`, `avg_daily_return`, `std_daily`,
`ann_vol`, `sharpe`, `oi_usd`, `oi_change`, `iv`, `iv_avg`, `iv_premium`,
`max_drawdown`, `win_rate`, `quote_volume`, `change_24h`, `last_price`,
`best_day`, `worst_day`, `days_used`.

## How the metrics are computed

All returns are simple percentage changes between consecutive daily closes.

| Metric | Definition |
| --- | --- |
| Daily return (avg) | Mean of daily percentage returns over the window |
| Cumulative return | Price change from the first to the last close in the window |
| SD (daily) | Sample standard deviation (n−1) of daily returns |
| Annualized volatility | Daily SD × √365 (crypto trades every day) |
| Sharpe | Mean daily return ÷ daily SD × √365, no risk-free rate |
| Max drawdown | Largest peak-to-trough decline of the daily close |
| Open interest | Notional USD value of outstanding futures contracts |
| Implied volatility | Deribit DVOL index, or a realized-volatility proxy |

## Data sources and their limits

Read this before trusting a number.

- **Prices and open interest** come from Binance USDT-M Futures. The symbol list
  is every trading USDT perpetual, ranked by 24-hour quote volume and cut at
  `UNIVERSE_SIZE`.
- **Open interest history is only about 30 days deep.** That is a Binance
  retention limit, not a bug. The OI column and OI chart always reflect that
  window even when you select 90 days or 1 year.
- **Implied volatility uses Deribit's DVOL index, which currently exists only
  for BTC and ETH.** Every other coin falls back to 30-day annualized realized
  volatility as a proxy, marked with an `IV proxy` badge in the table. A proxy is
  a backward-looking estimate — it is not an options-market implied volatility
  and should not be read as one.
- **Recently listed coins have short histories.** A coin listed six days ago can
  top a 1-year daily-return ranking. The last column shows how many days each row
  actually used, and the checkbox above the table hides coins without a full
  window. Summary tiles use medians for the same reason.
- Coins with fewer than three closed daily candles are skipped entirely; the
  status badge reports how many.
- If the exchange APIs cannot be reached, the app switches to generated demo data
  and says so in an amber badge. Those numbers are synthetic.

## Troubleshooting

**The table stays empty and shows `HTTP 499` or `Failed to fetch`, but the
server is running.** The request is being cancelled before it reaches Flask —
Flask and Werkzeug never emit status 499. This is almost always a browser
extension (ad blockers match paths such as `/api/metrics` against analytics
filter lists). Open the page in a private window to confirm, then allow
`127.0.0.1` in the extension. The default `/data/*` paths avoid the common
filter rules.

**`The token '&&' is not a valid statement separator in this version`.** You are
on Windows PowerShell 5.1, which does not support `&&`. Run each command on its
own line.

**The first load is slow.** Lower `UNIVERSE_SIZE`, or raise `REQUEST_DEADLINE`
if you would rather wait for a complete response. The terminal prints progress
as it works (messages are in Thai, matching the UI):

```
[dashboard] universe: 80 เหรียญ ใน 0.7 วินาที
[dashboard] metrics 90 วัน: พร้อม 78/80 เหรียญ (ข้าม 2, ค้าง 0) ใน 3.9 วินาที
```

The first line reports how many symbols were listed and how long it took; the
second reports how many are ready, skipped, and still loading.

**Charts do not render.** Chart.js is loaded from a CDN. Without internet access
the table still works; the app shows a notice instead of the charts.

## Disclaimer

This project is provided for educational and informational purposes only.

- **It is not financial, investment, or trading advice**, and nothing it
  displays constitutes a recommendation to buy or sell any asset.
- **The numbers may be wrong.** Data comes from third-party APIs that can be
  delayed, rate-limited, incomplete, or unavailable. Several metrics are
  approximations, most notably the implied-volatility proxy used for coins
  without a DVOL index. Do not use this dashboard as a source of truth for
  anything that matters.
- **Cryptocurrency and derivatives trading carries substantial risk of loss.**
  Past returns and historical volatility do not predict future results.
- **You are responsible for your own use of the exchange APIs.** Access to
  Binance and Deribit endpoints is governed by their respective terms of
  service, rate limits, and regional restrictions. This project is not
  affiliated with, endorsed by, or connected to either exchange.
- The software is provided "as is", without warranty of any kind. See
  [LICENSE](LICENSE).

Consult a licensed financial professional before making investment decisions.

## License

Released under the MIT License. See [LICENSE](LICENSE).

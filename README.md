# Sniper Flow

Order-flow sniper signals for **Binance USDT-M Futures**, delivered to Telegram.

The engine hunts one thing: high-grade support/resistance zones where price
arrives, takes out liquidity, gets absorbed by passive orders, and reverses.
It never places an order, never needs an exchange API key, and never touches
your funds — it reads public market data and tells you where the setup is.

```
Coin · Direction · Entry Zone · Stop Loss · TP1 / TP2 / TP3
```

---

## Quick start

```bash
pip install -r requirements.txt
cp .env.example .env          # fill in the two Telegram values
python main.py
```

That's it. Two environment variables are required:

| Variable | Where to get it |
|---|---|
| `TELEGRAM_BOT_TOKEN` | message [@BotFather](https://t.me/BotFather) → `/newbot` |
| `TELEGRAM_CHAT_ID` | message [@userinfobot](https://t.me/userinfobot), or your group's id |

Two extra flags are useful before you go live:

```bash
python main.py --check       # validate configuration, print every setting
python main.py --selftest    # run the offline logic tests (no network, no token)
```

The self-test builds a synthetic market that contains the target setup by
construction and asserts every stage fires — zones, sweep, absorption, CVD
divergence, MSS, risk model, database, monitor, message rendering.

---

## How it works

### 1. Universe — every 5 hours

All USDT perpetuals with **24h quote volume above $20M** (`MIN_QUOTE_VOLUME_USD`).
Thinner books can't produce readable footprints, so they aren't worth the
bandwidth.

### 2. Zone map — every 4 hours

600 × 4H and 600 × 1H candles per symbol. Fractal pivots are clustered by ATR
tolerance into zone boxes, then scored out of 100:

| Section | Points | What earns them |
|---|---|---|
| **A. HTF confluence** | 30 | 4H major zone **+20** · overlapping 1H zone **+10** |
| **B. Reaction quality** | 20 | strong prior rejection wick **+10** · impulsive displacement out **+10** |
| **C. Liquidity** | 20 | swing high/low in front **+5** · equal highs/lows **+5** · untapped sweep liquidity **+10** |
| **D. Volume / order-flow history** | 20 | significant volume **+5** · prior absorption **+5** · delta extreme + reversal **+5** · CVD divergence **+5** |
| **E. Freshness** | 10 | untouched **+10** · tested once **+5** · tested more **0** |

**80–100 → A+ · 70–79 → A · below 70 → discarded.**

A zone is thrown away regardless of score if it has been broken, tested three
or more times, or sits in the middle of the range. In practice ~100 raw levels
per chart collapse into 3–8 tradable zones.

> One documented deviation: the rubric defines "4H **major**" as +20 but says
> nothing about ordinary single-touch 4H zones. Those get **12** instead of 20
> here (`zones.py`, section A), so a fresh untested level stays viable without
> being handed the same weight as a proven one.

Historical delta and CVD come from the kline `takerBuyBaseVolume` field
(`delta = 2 × takerBuy − volume`), so 600 bars of order-flow history cost one
REST call instead of replaying millions of trades.

### 3. Proximity — every 5 minutes

A single `!markPrice@arr@1s` WebSocket carries live prices for *every* symbol,
so scanning the whole watchlist and monitoring open setups costs **zero REST
weight**. When price enters an A/A+ zone the symbol is **armed**.

### 4. Arming — order flow goes live

Only armed symbols open a per-symbol stream (`aggTrade` + `depth20@500ms` +
1m/3m/5m klines), seeded from REST so the engine isn't blind for the first
minutes. The stream closes the instant the symbol disarms — that's what keeps
memory flat over weeks.

### 5. Confirmation — the sequence, not the score

A great zone gets a symbol *watched*, never traded. The trade is born only when
the whole sequence prints:

```
4H demand  →  1H overlap  →  price enters the zone
           →  liquidity sweep below the prior swing low
           →  large negative delta
           →  footprint shows heavy aggressive selling
           →  absorption: price refuses to fall
           →  CVD divergence / reclaim
           →  price reclaims the swept level
           →  MSS on 5m / 3m
           →  LONG
```

Shorts are the exact mirror.

**Mandatory gates** — all must pass: enough flow to judge, sweep **and**
reclaim, absorption, CVD divergence or reclaim, MSS, order book not pulling
liquidity, footprint not a mess, not fighting a strong 4H trend.

**Optional confirmations** — at least two of: delta extreme, stacked footprint
imbalances, supportive book / consumed wall, reclaim strength, LTF structure
flip. These also feed a weighted confidence score which must clear
`MIN_CONFIDENCE` (0.62).

#### The interpretation rules the engine actually applies

**Delta.** Positive delta is not bullish by itself. At resistance, huge buy
delta with no upward progress means passive sellers are absorbing — bearish.
At support, huge sell delta with no downward progress means passive buyers are
absorbing — bullish. Absorption is measured as aggression concentrated at the
extreme of the window *versus* the price response to it.

**CVD.** Used for divergence and reclaim only, never as a trend indicator.
Bullish: price lower low while CVD higher low. Bearish: price higher high while
CVD lower high.

**Order book / heatmap.** Never standalone. Walls are tracked through time and
classified by fate:

- **Case A** — big bid sits there, price approaches, the bid vanishes → spoof → ignored.
- **Case B** — bid sits there, price arrives, market sells hit it, size is consumed, price holds and rebounds → real absorption → high interest.

Before entry the engine looks at concentration, stacking and whether liquidity
is being added or pulled. At entry the question is *"did orders actually
execute, and how did price respond?"* — not *"is size present?"*

**Skip conditions** (even for an A+ zone): completely against the 4H trend,
zone tested 3–4 times, no sweep, no absorption, no CVD divergence, book
liquidity constantly disappearing on approach, messy footprint with no clear
imbalance, zone in the middle of a range.

### 6. Risk model

- **Stop** sits beyond the sweep extreme (and beyond the zone), buffered by
  `SL_BUFFER_ATR` — the exact price where the setup is wrong, not a round number.
- **Entry zone** spans the reclaimed level to current price, padded slightly.
- **TP1 / TP2 / TP3** default to 1R / 2R / 3.5R, with TP3 optionally capped at
  the next opposing zone (`STRUCTURAL_TP_CAP`). If capping drops R:R below
  `MIN_RR_AFTER_CAP`, the signal is dropped instead of downgraded.

### 7. Monitoring until the setup is finished

Every signal is tracked from the moment it fires until **TP3 or SL**:

- TP1 hit → alert, stop moves to breakeven
- TP2 hit → alert, stop trails to TP1
- TP3 or SL → alert, setup closed, result recorded in R
- No progress after `TRADE_TTL_HOURS` (48h) → expired and closed

When a setup finishes the coin **returns to the watchlist pool** after a
cooldown, and its zones are rebuilt immediately.

Results are accounted in **R multiples** — multiples of the risk the signal
itself defined. No position sizing is assumed, because nothing is executed.

---

## Telegram commands

| Command | What it does |
|---|---|
| `/status` | engine state + open setups |
| `/active` | detailed open setups with MFE/MAE |
| `/watchlist` | current volume-filtered universe |
| `/zones SYMBOL` | graded zones with full score breakdown |
| `/signals [n]` | recent signals |
| `/pnl [today\|week\|month\|all]` | performance in R |
| `/report [period]` | full report incl. per-symbol and per-grade breakdown |
| `/stats` | internals, armed symbols, most common rejection reasons |
| `/health` | connections, rate-limit usage, memory, DB size |
| `/close ID` | force-close a setup at market price |
| `/pause` · `/resume` | stop/start generating new signals |
| `/help` | command list |

`/stats` is the one to watch early on — it shows *why* setups are being
rejected, which tells you whether your thresholds match current conditions.

---

## Rate limits and memory

Binance allows 2400 request-weight per minute per IP. The engine runs on a
self-imposed budget (`WEIGHT_BUDGET_PER_MIN`, default **1100**) and backs off
hard on 418/429.

Accounting is anchored to the exchange's own `X-MBX-USED-WEIGHT-1M` header:
pressure is the last reading plus whatever has been spent since it was taken.
That matters because Binance meters in fixed one-minute buckets that reset on
the boundary — a purely local sliding window keeps charging for requests the
exchange has already forgotten, and will stall the engine against pressure that
no longer exists.

Weight is also **prioritised**. Background work — the periodic zone rebuild and
the 24h ticker sweep — is capped at 65% of the budget, so prices, arming and
trade monitoring always have headroom. A full rebuild of 220 symbols is ~2200
weight; the limiter paces it across a few minutes rather than letting it freeze
everything else for a minute.

Memory stays flat because:

- footprint buckets older than the window are dropped on every insert
- the wall registry is capped and pruned by recency
- per-symbol streams exist only while armed, and state is disposed on disarm
- mark prices are pruned to the current watchlist
- explicit `gc.collect()` every housekeeping cycle, with RSS logged

Typical VPS footprint: **~90–140 MB** with a dozen symbols armed.

---

## Configuration

Everything is env-tunable — see `.env.example` for the common knobs and
`config.py` for the full list (~90 settings). The ones worth touching first:

| Setting | Default | Effect |
|---|---|---|
| `MIN_QUOTE_VOLUME_USD` | 20000000 | universe size |
| `SCORE_A` / `SCORE_A_PLUS` | 70 / 80 | how selective the zone map is |
| `MIN_CONFIDENCE` | 0.62 | how selective the confirmation engine is |
| `MIN_OPTIONAL_CONFIRMS` | 2 | secondary confluence required |
| `MAX_ARMED_SYMBOLS` | 12 | concurrent order-flow streams |
| `MAX_ACTIVE_TRADES` | 8 | concurrent open setups |
| `TP3_R` | 3.5 | final target in R |
| `RESPECT_HTF_TREND` | true | block setups against a strong 4H trend |

Loosen `MIN_CONFIDENCE` and `SCORE_A` in quiet markets; tighten them when
you're getting more signals than you want to watch.

---

## Deploying on a VPS

```bash
sudo cp deploy/sniper-flow.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sniper-flow
journalctl -u sniper-flow -f
```

Edit the paths and user in the unit file first. See `deploy/DEPLOY.md` for the
full walkthrough including a non-systemd option.

---

## Project layout

```
sniper_flow/
├── main.py                 entry point, --selftest / --check
├── config.py               every tunable, env-overridable
├── selftest.py             offline synthetic-market test suite
├── requirements.txt        aiohttp · websockets · python-dotenv
├── .env.example
├── core/
│   ├── engine.py           orchestrator, 7 async loops, command handler
│   ├── watchlist.py        volume filter + symbol metadata
│   ├── zones.py            S/R detection + the 100-point rubric
│   ├── indicators.py       ATR, pivots, EMA, CVD, trend
│   ├── structure.py        sweeps, reclaims, MSS
│   ├── orderflow.py        footprint, delta, absorption, imbalance
│   ├── orderbook.py        walls, spoof-pull vs consumption
│   ├── armed.py            per-symbol live context
│   ├── confirm.py          confirmation engine + confidence
│   ├── risk.py             entry / SL / TP construction
│   ├── monitor.py          SL/TP tracking to completion
│   ├── database.py         SQLite (WAL) persistence + reporting
│   ├── rest.py             weight-aware Binance REST client
│   ├── ws.py               mark-price + per-symbol streams
│   ├── rate_limiter.py     sliding-window weight budget
│   ├── models.py           Candle / Zone / Decision / Signal
│   └── utils.py            logging, math, formatting, memory
├── notifier/
│   ├── telegram.py         Bot API client, long polling
│   └── formatter.py        message templates
└── deploy/
    ├── sniper-flow.service systemd unit
    └── DEPLOY.md           VPS walkthrough
```

---

## When the network fights back

Some hosts — cloud instances in restricted regions especially — accept the
Binance WebSocket handshake, answer pings, and then never deliver a single
payload. At the transport layer that is indistinguishable from a healthy
connection, so the engine watches for *data*, not connectivity:

- a socket that goes quiet past `WS_IDLE_TIMEOUT_SEC` is torn down and retried
- the mark-price stream rotates through three endpoint spellings before
  concluding the network is the problem
- prices fall back to `ticker/price` (weight 2 for every symbol), cached
- armed symbols fall back to REST order-flow polling every `FLOW_POLL_SEC`,
  capped at `MAX_ARMED_FALLBACK` symbols so a dead network cannot burn the
  weight budget
- repeatedly silent endpoints get an escalating backoff instead of a hammering
- `/health` states plainly which transport is in use

**Signals still fire in REST-only mode**, at coarser flow resolution. What the
engine will *not* do is trade stale data: if order flow has not advanced within
`MAX_FLOW_AGE_SEC`, every setup is blocked with `stale_flow`, and a context
whose feed cannot be revived is disarmed rather than left scoring a frozen
snapshot.

---

## Disclaimer

This is an analysis and alerting tool. It produces trade *ideas* from public
market data — it does not place orders, manage positions, or size risk, and
nothing it outputs is financial advice. Signal quality depends entirely on the
thresholds you configure and the market you point it at. Test it on paper long
enough to trust the numbers in `/report` before risking anything real.

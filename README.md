# Sniper Flow

Order-flow sniper signals for **Binance and Bybit USDT perpetuals**, delivered
to Telegram.

The engine hunts one thing: the manipulation pattern. Price is driven into a
pocket of resting liquidity, pushed just beyond it to trigger the stops sitting
there, absorbed by passive size while retail gets flushed, and then reversed
toward the larger pool on the other side. That is how execution desks fill
size, and the whole system is built to identify it early enough to ride
alongside rather than get swept with everyone else.

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

### 3. Multi-exchange feed — Binance + Bybit

The watchlist is split evenly between the two venues. 100 symbols means 50
streamed from each, which halves the websocket load and connection count per
IP and means one venue rate-limiting or blocking you degrades half the
coverage instead of all of it. Symbols listed on only one venue route there
automatically.

Every venue difference is resolved in an adapter, so nothing downstream knows
where a print came from:

| | Binance | Bybit |
|---|---|---|
| Trade side | flags the **maker** | flags the **taker** |
| Order book | full snapshots | snapshot + deltas (reconstructed locally) |
| Intervals | `5m` | `5` |
| Taker volume in klines | yes | no |

That last row is why candle history always comes from `HISTORY_EXCHANGE`
(Binance): historical delta and CVD are derived from taker-buy volume. Bybit
supplies the live tape, and when a Bybit symbol needs a CVD series the engine
synthesises minute candles from the tape instead.

### 4. The queue — websockets never compute

This is the rule the whole data path is built around:

```
socket coroutine  ──►  normalise  ──►  asyncio.Queue  ──►  worker  ──►  analytics
     (no maths)                        (sharded)          (all maths)
```

A websocket callback parses the frame, turns it into a normalised event, drops
it in a queue and returns. Delta, CVD, footprint, heatmap and absorption maths
all happen in separate worker tasks.

Doing analytics inline is the classic way to destroy a feed: aggTrade and book
frames arrive in bursts of hundreds per second across a dozen symbols, the
receive buffer backs up behind your maths, frames queue in the kernel, and the
feed falls minutes behind **while still looking perfectly connected**. By the
time the strategy sees a print it is fiction.

Queues are sharded by symbol, so each symbol's events stay strictly ordered
within one worker while workers run independently. Under pressure a full shard
drops its **oldest** event, never blocking the socket — current market state
matters more than a complete history.

Measured on the target box: **620,000 events/sec end-to-end**, against a
realistic load of ~720/sec for 18 armed symbols. Roughly 865x headroom. `/health`
reports queue depth, drops and event lag so you can see it directly.

### 5. Confirmation — liquidity first, everything else as evidence

The order of these checks is the strategy:

```
where does the money rest?   →  is price being driven into liquidity?
  →  was that liquidity actually taken (recent sweep)?
  →  did someone absorb the aggression at the extreme?
  →  is a real participant working there (iceberg / TWAP)?
  →  did price reclaim and shift structure?
```

A spike in absorption, delta, CVD or aggression proves nothing on its own —
that momentum evaporates in seconds. It only means something when it happens
at a level where liquidity was resting, in the direction of the larger pool
price still has to travel to. So liquidity context is evaluated **first**, and
everything else is treated as evidence about it.

**Where the money rests.** Two maps answer this. The structural map labels
every swing HH / HL / LH / LL and marks which are still untapped — a prior
swing is a stop cluster, and untapped clusters are fuel. The heatmap measures
resting size over time from DOM snapshots. If the bulk of untapped liquidity
sits against the trade and no heatmap shelf defends the level, the setup is
rejected with `liquidity_rests_against_trade`. If there is nowhere for price
to travel to at all, it is rejected with `no_liquidity_target`.

**The sweep must be recent.** Three to five candles, no more. A sweep thirty
bars back is stale: that liquidity is already taken, the level protects
nothing, and price will slice straight through it on the next drive. Sweeps
outside the window are rejected with `sweep_stale`, and by default the sweep
must have taken out a *labelled structural level* rather than a random wiggle.

**Institutional participation.** A desk with size cannot lift the book, so it
slices. Each method leaves a fingerprint the engine looks for:

| | Signature |
|---|---|
| **TWAP** | one repeated clip size at metronomic intervals |
| **VWAP** | clips scaling with volume, price hugging the VWAP line |
| **Iceberg** | executed volume at one price far exceeding the size ever displayed, with repeated refills |

Regularity is measured with median absolute deviation, not standard deviation
— a real tape sprinkles unrelated trades of the same size between an
algorithm's clips, and a handful of those outliers destroys a mean-based
statistic while leaving the rhythm obvious to a median-based one.

**Order book / heatmap.** Never standalone. Walls are tracked through time and
classified by fate:

- **Case A** — big bid sits there, price approaches, the bid vanishes → spoof → ignored.
- **Case B** — bid sits there, price arrives, market sells hit it, size is consumed **or refilled**, price holds → real absorption → high interest.

A refilling wall is the strongest form of Case B: it is an iceberg defending
the level.

**Multi-timeframe.** 4H zone and trend, 1H overlap, 15m and 3m structure bias,
5m/3m sweep and MSS. If every lower timeframe opposes the trade it is rejected
outright.

**Counter-trend.** This is deliberately strict, because trading longs into a
4H downtrend is the fastest way to donate money. Against a *strong* trend the
setup is blocked outright. Against a soft trend it needs **an A+ zone, a fresh
structural sweep and institutional participation**, plus a confidence penalty.

### 6. Risk model — stops at the wick, targets at liquidity

**The stop goes just beyond the wick of the sweep that just happened.** Not
below an older sweep — that liquidity is gone and price will cut through it.
The recent wick is the price at which the setup is genuinely wrong.

**Targets go where liquidity rests, not at round R multiples.** Price travels
toward resting size, so TP1/TP2/TP3 are placed at the heatmap shelves,
untapped swing pools and opposing zones ahead of the trade, each one required
to still clear a minimum reward (`TP1_MIN_R`, `TP2_MIN_R`, `TP3_MIN_R`). R
multiples remain as a fallback when the map is empty. The signal card names
the source of every target, so you can see whether TP2 is a heatmap wall or an
untapped HL.

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

Measured footprint: **~40 MB** with 18 armed symbols (0.4 MB each), and a full
evaluation cycle costs 0.7 ms per symbol — about 0.1% of one core at the
default 15-second interval. Comfortable on a 2-core / 4GB VPS with room to
raise `MAX_ARMED_SYMBOLS` well beyond the default.

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
| `TP3_MIN_R` | 2.8 | minimum reward TP3 must clear |
| `COUNTER_TREND_POLICY` | strict | `allow` · `strict` · `block` |
| `SWEEP_MAX_AGE_BARS` | 5 | how recent a sweep must be to count |
| `EXCHANGES` | binance,bybit | venues to split the watchlist across |
| `REQUIRE_INSTITUTIONAL` | false | demand iceberg/TWAP evidence on every signal |
| `RESPECT_HTF_TREND` | true | block setups against a strong 4H trend |

Loosen `MIN_CONFIDENCE` and `SCORE_A` in quiet markets; tighten them when
you're getting more signals than you want to watch.

If you are getting counter-trend signals you dislike, set
`COUNTER_TREND_POLICY=block`. If you want only setups with a visible
institutional participant, set `REQUIRE_INSTITUTIONAL=true` — it will cut
signal count substantially.

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
│   ├── engine.py           orchestrator, 8 async loops, command handler
│   ├── events.py           normalised TradeEvent / DepthEvent / KlineEvent
│   ├── feed.py             sharded queues, workers, venue router, price book
│   ├── heatmap.py          Bookmap-style liquidity heatmap from DOM
│   ├── liquidity.py        HH/HL/LH/LL structural liquidity map
│   ├── execution_algos.py  TWAP / VWAP / iceberg detection
│   ├── exchanges/          adapters: base · binance · bybit
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
│   ├── ws.py               mark-price + session-driven streams
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

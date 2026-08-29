# Changelog

## 2.0.1 — from the first live 2.0 run

The 2.0 feed worked: 129 symbols split binance=65 / bybit=64, and **Bybit
websockets connected on a host that cannot reach Binance's** — exactly the
redundancy the split was built for. Three things were wrong around it.

**7. The rate limiter shouted once a second for a minute.**
`waiting 44s... 44s... 45s...` was five concurrent zone builds each logging
from a counter-based throttle (`_waits % 8`), so whichever task happened to
tick produced a line. Now throttled by time: one line every 30 seconds, at
INFO, saying plainly that background work is being paced.

**8. The zone rebuild was still a burst.** 129 symbols x 2 timeframes x 600
candles is 1290 request weight against a 715 bulk cap - two full minutes of
saturation at every refresh. Zones move on the scale of hours, so the periodic
rebuild is now a **rolling refresh**: the single stalest symbol is rebuilt
every few seconds, spreading identical work into ~5 weight/min that the budget
never notices. The initial build remains a burst by necessity, but it is paced,
volume-ordered, logs progress instead of spam, and priority calls are served
throughout it (measured: 240 price calls during a 120s build, none starved).

**9. Junk listings reached the strategy.** The run armed `MRVLUSDT` and
`SPCXUSDT` (tokenised equities), `XAGUSDT` and `PAXGUSDT` (metals), and a meme
contract with a CJK ticker. They pass every volume filter but track an
underlying the engine cannot see, on books far too thin for footprint work.
The universe now requires a plain ASCII ticker, rejects a configurable list of
non-crypto underlyings, and requires a listing on the history venue - without
its klines a symbol can never be scored, and attempting it wasted weight.

Also: **4xx responses are no longer retried** (an unlisted symbol stays
unlisted; three attempts burned weight for a guaranteed failure), zone grading
tightened so an ordinary labelled swing no longer collects the full major
bonus (568 zones from 127 symbols put 32 symbols in a zone at once), and a
mark price stream that has failed on every endpoint is now **abandoned** rather
than reconnected forever.

## 2.0.0 — institutional edition

Rebuilt around one question: *where is the resting liquidity, and is price
being driven into it?* Everything else became evidence about that question
rather than a signal in its own right.

### Multi-exchange feed

- **Binance + Bybit**, watchlist split evenly between them. Halves websocket
  load and connection count per IP; one venue degrading costs half the
  coverage instead of all of it. Symbols listed on one venue route there.
- Venue dialects resolved in adapters: Binance flags the maker side, Bybit the
  taker; Binance sends book snapshots, Bybit sends deltas (reconstructed and
  throttled locally); intervals normalised; Bybit's 10-topic subscribe cap and
  application-level ping handled.
- Bybit klines carry no taker-buy volume, so candle history comes from
  `HISTORY_EXCHANGE`. When a CVD series is needed for a Bybit symbol it is
  synthesised from the live tape instead.

### Queue architecture

- Websockets **only enqueue**. All delta / CVD / footprint / heatmap maths runs
  in separate worker tasks, so a burst of prints can never stall the socket
  that produced them and leave the feed minutes behind while looking connected.
- Queues sharded by symbol: per-symbol ordering preserved, workers independent.
- Under pressure the oldest event is dropped, never the newest, and the socket
  never blocks. Measured 620k events/sec end-to-end — ~865x realistic load.
- Queue depth, drops and event lag reported in `/health`.

### Liquidity

- **Heatmap** built from DOM snapshots: accumulates `size x seconds` per price
  bucket with a decay half-life, so persistent shelves outweigh flashed size.
  Buckets are classified consumed / pulled / refilled / resting.
- **Structural map** labels every swing HH / HL / LH / LL, tracks which are
  still untapped, and clusters equal highs/lows. These become S/R zones in
  their own right — price reversed there, so orders rest there.
- **Liquidity-first gate**: a setup is rejected if there is no pool to travel
  to (`no_liquidity_target`) or if the untapped bulk sits against the trade
  with no heatmap shelf defending the level (`liquidity_rests_against_trade`).

### Execution algorithms

- TWAP, VWAP and iceberg detection from the tape. Regularity measured with
  median absolute deviation, so noise trades of the same size cannot mask an
  algorithm's rhythm.
- Every clip size with enough prints is scored, not just the most frequent —
  a busy tape produces noisy buckets that outnumber a quiet algorithm.
- Iceberg = executed volume at a price far exceeding the size ever displayed
  there. Refilling book walls now count as the strongest form of Case B.

### Sweeps, stops and targets

- Sweep must be **3-5 candles old**, and by default must have taken out a
  labelled structural level. Stale sweeps are rejected outright.
- Stop goes beyond **that** sweep's wick, never an older one.
- TP1/TP2/TP3 placed at liquidity pools — heatmap shelves, untapped swing
  pools, opposing zones — each still clearing a minimum reward. The signal card
  names the source of every target.

### Selectivity

Aimed squarely at the reported live result (two counter-trend longs stopped
out in a downtrend):

- `COUNTER_TREND_POLICY=strict` — against a soft trend a setup needs an A+
  zone, a fresh structural sweep and institutional participation, plus a
  confidence penalty. Against a strong trend it is blocked outright.
- Multi-timeframe gate: 4H trend, 1H overlap, 15m and 3m structure. All lower
  timeframes opposed = rejected.
- `MIN_OPTIONAL_CONFIRMS` raised to 3, `MIN_CONFIDENCE` to 0.65.

### Bugs found during the audit

- **`DepthTracker` trusted book ordering.** Best bid/ask were read from index
  0; an unsorted snapshot poisoned the mid price and with it every distance,
  touch and wall verdict. Now derived from the levels themselves, with a
  crossed-book guard.
- **One corrupt timestamp could wipe the footprint book.** The rolling window
  prunes relative to the newest print, so a clock-skewed venue or a seconds
  value where milliseconds were expected erased everything in a single call.
  Implausible timestamps are now rejected and counted.
- **Structural zones distorted the map.** Merging them into pivot zones
  widened the boxes, inflating touch counts and wrongly ageing fresh zones.
  They now enrich the existing zone instead.
- Superseded modules removed (`core/rest.py`, `ws.SymbolStream`).

### Tests

40 → **64 checks**, covering venue normalisation, delta-book reconstruction,
queue overflow, the 50/50 split, heatmap pools, TWAP and iceberg detection,
the liquidity gate, stale-sweep rejection and corrupt-timestamp handling.

## 1.0.2 — rate limiter, round two

**5. Local accounting fought the exchange's own numbers.** Binance meters in
fixed one-minute buckets that reset on the boundary; the limiter used a sliding
60-second window and throttled against pressure that no longer existed. The
header is now ground truth: last reading plus what has been spent since.

**6. The zone rebuild monopolised the budget** — 880 weight in 8 seconds on a
1100 budget. Request weight is now prioritised, with background work capped at
`BULK_WEIGHT_SHARE` so prices, arming and monitoring always have headroom.

## 1.0.1 — bug hunt

**1. Rate limiter latched permanently**, freezing every REST call and silencing
Telegram (the poll loop was stuck awaiting a blocked handler).
**2. A hung command could silence the bot permanently** — commands now run as
tasks under a timeout.
**3. Silent websockets were treated as healthy** — idle watchdog plus endpoint
rotation.
**4. Stale order flow could produce a signal** — blocked past `MAX_FLOW_AGE_SEC`.

Plus REST fallback for prices and order flow, cooldowns surviving restart, and
alert failures no longer stranding trades.

## 1.0.0

Initial release.

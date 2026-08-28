# Changelog

## 1.0.2 — rate limiter, round two

Found from a second live run: `waiting 20s on the weight budget
(local=1099 reported=1 budget=1100)` followed by `REST price fallback timed out`.

**5. Local accounting fought the exchange's own numbers.**
Binance meters in fixed one-minute buckets that reset on the boundary; the
limiter used a *sliding* 60-second window. After the startup burst the window
still held 1099 while the exchange had already reset to 1, so the engine
throttled itself against pressure that no longer existed — stalling prices,
arming and monitoring for a full minute.
*Fix:* the header is now treated as ground truth. Pressure is measured as the
last reading plus only what has been spent since it was taken, which is both
accurate and self-correcting. The sliding window survives as a fallback for
when no reading is available.

**6. The zone rebuild monopolised the entire budget.**
88 symbols x 2 timeframes x 600 candles = 880 weight in about 8 seconds, on a
1100 budget — nothing left for prices, arming or the trade monitor.
*Fix:* request weight is now prioritised. Background work (the periodic zone
rebuild, the 24h ticker sweep) is capped at `bulk_share` of the budget (65% by
default), permanently reserving headroom for latency-sensitive calls. The
rebuild simply takes a little longer instead of freezing the engine.

Also: the REST price fallback warning is throttled to once per five minutes,
and websocket reconnect logging goes quiet after six silent endpoints with one
clear explanation instead of a warning every 45 seconds.

## 1.0.1 — bug hunt

Fixes found from a live deployment where Telegram commands stopped responding
and the mark price stream reported `stale (1000000000s)` every 15 minutes.

### Critical

**1. Rate limiter latched permanently, freezing every REST call.**
`X-MBX-USED-WEIGHT-1M` was stored without a timestamp and never decayed. After
the initial zone rebuild the reading froze at 1097 against a 1100 budget, so
`acquire()` waited for headroom that could never appear — and since only a
successful request can refresh the reading, the state was self-sustaining.

This is what silenced Telegram: `/status` → `_prices()` → REST fallback →
`acquire()` → blocked forever. The poll loop was stuck awaiting the handler, so
no command ever replied. The trade monitor was frozen by the same call.
*Fix:* the reading now decays linearly across the exchange's minute.

**2. A hung command could silence the bot permanently.**
Commands were awaited inline in the polling loop. *Fix:* each command runs as
its own task under `COMMAND_TIMEOUT_SEC`, and replies with a timeout notice
instead of going quiet.

**3. Silent websockets were treated as healthy.**
A socket that connects, answers pings and delivers nothing is identical to a
working one at the transport layer. `async for` simply waited forever.
*Fix:* an idle-timeout watchdog treats silence as failure, the mark-price
stream rotates through three endpoint spellings, and repeatedly silent
endpoints get escalating backoff.

**4. Stale order flow could produce a signal.**
With a dead feed, armed contexts kept re-scoring their seed data — six symbols
sat armed for 75 minutes in the reported deployment. A signal built on
90-minute-old flow was possible. *Fix:* setups are blocked with `stale_flow`
past `MAX_FLOW_AGE_SEC`, and unrevivable contexts are disarmed.

### Resilience

- **REST fallback for order flow.** Armed symbols poll aggTrades, klines and
  depth when their socket is silent, capped at `MAX_ARMED_FALLBACK`.
- **Cheaper price fallback.** `ticker/price` (weight 2, all symbols) replaces
  `premiumIndex` (weight 10), with a short cache so the 2s monitor tick cannot
  spam REST.
- **Degraded mode is self-limiting.** Concurrent armed symbols tighten
  automatically, and sockets are not opened at all once the transport is proven
  blocked.
- **`/health` and `/stats`** now report price source, feed age, silent sockets,
  REST poll counts and rate-limit waits.

### Correctness

- Alert delivery failures can no longer strand a trade or keep its symbol out
  of the hunting pool.
- Re-arm cooldowns survive a restart (one batched query), preventing a
  duplicate signal on the same move.
- One symbol raising during evaluation no longer skips the remaining symbols.
- Trade dedup tolerates a missing aggregate-trade id, and a quiet tape is no
  longer mistaken for a dead feed.
- `/zones` no longer raises on a symbol with an empty zone list.
- `websockets` upper pin removed — verified on 12.x through 17.x.

### Tests

Self-test grew from 33 to 40 checks, with a resilience section covering the
limiter decay, budget enforcement, silent-socket detection, REST feed revival
and command timeouts.

## 1.0.0

Initial release.

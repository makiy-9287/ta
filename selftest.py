"""
Offline self-test.

Builds a synthetic market that contains, by construction, exactly the setup
this engine is supposed to find:

    a 4H demand zone with clustered lows, rejection wicks, displacement out,
    untapped liquidity below it, and a 1H zone sitting on top of it
      -> price returns, sweeps the prior swing low, gets absorbed by passive
         bids, CVD diverges, price reclaims and shifts structure on 3m/5m

Then it runs the real detection code over that data and asserts each stage
fires. No network, no Telegram token, no database credentials required:

    python main.py --selftest
"""
from __future__ import annotations

import asyncio
import os
import random
import tempfile
import time
from typing import List, Tuple

from config import SETTINGS
from core.armed import ArmedContext
from core.confirm import evaluate, liquidity_targets
from core.events import DepthEvent, TradeEvent
from core.execution_algos import detect_iceberg, detect_slicing, session_vwap
from core.heatmap import LiquidityHeatmap
from core.liquidity import LiquidityMap
from core.database import Database
from core.indicators import atr, cvd_divergence, htf_trend
from core.models import Candle
from core.monitor import TradeMonitor
from core.risk import build_signal
from core.structure import detect_mss, detect_sweep
from core.utils import get_logger
from core.zones import ZoneEngine

log = get_logger("selftest")
random.seed(7)

PASS, FAIL = "  \033[92mPASS\033[0m", "  \033[91mFAIL\033[0m"
_results: List[Tuple[str, bool, str]] = []


def check(name: str, ok: bool, note: str = "") -> bool:
    _results.append((name, bool(ok), note))
    print(f"{PASS if ok else FAIL}  {name}" + (f"  ({note})" if note else ""))
    return bool(ok)


# ----------------------------------------------------------------- generators
def _candle(ts: int, o: float, c: float, spread: float, vol: float,
            buy_ratio: float, low_wick: float = 0.0, high_wick: float = 0.0) -> Candle:
    hi = max(o, c) + spread + high_wick
    lo = min(o, c) - spread - low_wick
    return Candle(ts=ts, open=o, high=hi, low=lo, close=c, volume=vol,
                  quote_volume=vol * c, trades=int(vol), taker_buy=vol * buy_ratio,
                  close_ts=ts + 1)


def zigzag(points: List[float], per_leg: int, start_ts: int, interval_ms: int,
           base_vol: float = 1000.0) -> List[Candle]:
    """Legs between turning points, with rejection wicks and displacement."""
    candles: List[Candle] = []
    ts = start_ts
    for li in range(len(points) - 1):
        a, b = points[li], points[li + 1]
        up = b > a
        for i in range(per_leg):
            o = a + (b - a) * (i / per_leg)
            c = a + (b - a) * ((i + 1) / per_leg)
            noise = abs(b - a) / per_leg * 0.25
            spread = noise + abs(o) * 0.0008
            vol = base_vol * random.uniform(0.75, 1.25)
            ratio = 0.62 if up else 0.38
            last = i == per_leg - 1
            lw = hw = 0.0
            if last and not up:                       # trough: long rejection wick
                lw = abs(b - a) * 0.10 + abs(o) * 0.0012
                c = c + (spread * 1.5)
                vol *= 2.8
                ratio = 0.28                          # heavy aggressive selling
            elif last and up:                         # peak
                hw = abs(b - a) * 0.10
                vol *= 1.8
                ratio = 0.70
            candles.append(_candle(ts, o, c, spread, vol, ratio, lw, hw))
            ts += interval_ms
        # displacement candle out of the turn
        if li + 2 <= len(points) - 1:
            nxt = points[li + 2]
            o = b
            c = b + (nxt - b) * 0.35
            candles.append(_candle(ts, o, c, abs(c - o) * 0.12, base_vol * 2.2,
                                   0.72 if c > o else 0.28))
            ts += interval_ms
    return candles


def build_htf() -> List[Candle]:
    """4H: a range that keeps rejecting the 100.0-100.35 area."""
    pts = [116, 104, 100.25, 113, 101.0, 114.5, 100.15, 112.0, 100.10, 106.0, 100.9]
    return zigzag(pts, 26, start_ts=1_700_000_000_000, interval_ms=4 * 3600 * 1000)


def build_mtf() -> List[Candle]:
    """1H: same demand area, so the zones overlap."""
    pts = [110, 100.30, 108, 100.20, 107, 100.18, 104, 100.9]
    return zigzag(pts, 34, start_ts=1_700_000_000_000, interval_ms=3600 * 1000)


def build_ltf(zone, interval_ms: int, bars: int) -> List[Candle]:
    """
    Descent into the detected zone -> sweep of the prior swing low -> reclaim.
    Everything is expressed relative to the real zone so the lower-timeframe
    story lines up with the higher-timeframe map.
    """
    lo, hi, h = zone.low, zone.high, zone.height
    legs = [
        (hi + 2.6 * h, hi + 1.5 * h, 0.24),   # trend down toward the zone
        (hi + 1.5 * h, hi + 1.1 * h, 0.07),   # minor pullback -> swing high
        (hi + 1.1 * h, lo + 0.30 * h, 0.22),  # into the zone -> prior swing low
        (lo + 0.30 * h, lo + 0.85 * h, 0.11), # weak bounce
        (lo + 0.85 * h, lo - 0.22 * h, 0.15), # THE SWEEP
        (lo - 0.22 * h, hi + 0.45 * h, 0.06), # reclaim: only a few bars ago
    ]
    total = sum(w for *_, w in legs)
    ts = 1_700_000_000_000
    out: List[Candle] = []
    for a, b, weight in legs:
        n = max(4, int(bars * weight / total))
        up = b > a
        for i in range(n):
            o = a + (b - a) * (i / n)
            c = a + (b - a) * ((i + 1) / n)
            jitter = abs(b - a) / n * random.uniform(-0.12, 0.12)
            c += jitter
            spread = abs(b - a) / n * 0.30 + h * 0.01
            vol = random.uniform(80, 140)
            ratio = 0.60 if up else 0.40
            lw = 0.0
            if not up and i == n - 1:
                lw = h * 0.05
                ratio = 0.22
                vol *= 2.5
            out.append(_candle(ts, o, c, spread, vol, ratio, lw))
            ts += interval_ms
    return out


def build_micro_cvd(zone, bars: int = 110) -> List[Candle]:
    """
    1m series engineered for a bullish CVD divergence: the first low prints on
    brutal aggressive selling, the second (lower) low on far weaker selling,
    so cumulative delta makes a higher low while price makes a lower low.
    """
    lo, hi, h = zone.low, zone.high, zone.height
    legs = [
        (hi + 1.4 * h, lo + 0.75 * h, 34, 0.30),   # heavy aggressive selling
        (lo + 0.75 * h, hi + 0.90 * h, 20, 0.72),  # buyers step in
        (hi + 0.90 * h, lo - 0.22 * h, 34, 0.455), # lower low, weak selling
        (lo - 0.22 * h, hi + 0.45 * h, 22, 0.70),  # reclaim
    ]
    ts = 1_700_000_000_000
    out: List[Candle] = []
    for a, b, n, ratio in legs:
        for i in range(n):
            o = a + (b - a) * (i / n)
            c = a + (b - a) * ((i + 1) / n) + abs(b - a) / n * random.uniform(-0.08, 0.08)
            vol = random.uniform(90, 110)
            out.append(_candle(ts, o, c, h * 0.006, vol, ratio))
            ts += 60_000
    return out


def build_agg_trades(zone, ctx_ts: int) -> List[TradeEvent]:
    """
    Order flow for the sweep window.

    Deliberately contains an institutional signature: a passive bid absorbing
    a flush at the lows, refilled in uniform clips at regular intervals - the
    footprint of an iceberg being worked, not a momentary spike.
    """
    lo, hi, h = zone.low, zone.high, zone.height
    sweep_low = lo - 0.22 * h
    reclaim = hi + 0.45 * h
    step = max(0.01, round(h * 0.05, 4))
    trades: List[TradeEvent] = []
    ts = ctx_ts - 22 * 60 * 1000

    def push(price: float, qty: float, buy: bool, gap: int = 900):
        nonlocal ts
        trades.append(TradeEvent("binance", "TESTUSDT", round(price, 6),
                                 round(qty, 4), buy, ts, len(trades) + 1))
        ts += gap

    for i in range(120):                                   # descent
        push(lo + 0.85 * h - i * (h * 0.009), random.uniform(1, 3), False)

    # the flush, absorbed at the lows - aggressive sells hitting a passive bid
    for i in range(320):
        price = sweep_low + random.choice([0, 1, 2, 3, 4]) * step
        push(price, random.uniform(9, 18), False, 400)
        if i % 5 == 0:
            push(price + step, random.uniform(1.5, 3), True, 200)

    # the institutional tell: identical clips at metronomic intervals.
    # 5.0 is chosen so no other leg of this fixture can produce it, making the
    # assertion about the detector rather than about the random seed.
    for i in range(26):
        push(sweep_low + step, 5.0, True, 3000)

    price = sweep_low + 2 * step                           # the reclaim
    while price < reclaim:
        for _ in range(9):
            push(price, random.uniform(6.2, 9.0), True, 250)
        push(price - step * 0.2, random.uniform(0.2, 0.5), False, 120)
        price += step * 1.4
    for _ in range(40):
        push(reclaim, random.uniform(2, 5), True, 300)
    return trades


def feed_depth(ctx, zone, ts: int) -> None:
    """
    A real bid wall that is touched, eaten and refilled - order book case B,
    and the heatmap shelf a long leans on. Also seeds an ask-side pool above
    for the targets to aim at.
    """
    lo, hi, h = zone.low, zone.high, zone.height
    wall_price = lo - 0.10 * h
    target_price = hi + 1.6 * h
    for i in range(60):
        mid = (lo + 0.85 * h) - i * (h * 0.03) if i < 20 else wall_price + (i - 20) * (h * 0.05)
        # the wall is consumed, then refilled: an iceberg working the level
        wall_qty = 900.0 if i < 12 else (max(80.0, 900.0 - (i - 11) * 90.0) if i < 22 else 780.0)
        bids = sorted([(round(wall_price, 6), wall_qty)] + [
            (round(mid - h * 0.02 * (k + 1), 6), random.uniform(40, 90)) for k in range(19)],
            key=lambda x: -x[0])
        asks = sorted([(round(target_price, 6), 850.0)] + [
            (round(mid + h * 0.02 * (k + 1), 6), random.uniform(25, 55)) for k in range(19)],
            key=lambda x: x[0])
        ev = DepthEvent("binance", "TESTUSDT", bids, asks, ts + i * 1000)
        ctx.on_depth(ev)


# ---------------------------------------------------------------------- tests
HTF_LIQUIDITY = None


def test_zones() -> Tuple[bool, object]:
    global HTF_LIQUIDITY
    htf, mtf = build_htf(), build_mtf()
    engine = ZoneEngine(SETTINGS)
    zones = engine.build("TESTUSDT", htf, mtf)
    HTF_LIQUIDITY = engine.last_liquidity
    demand = [z for z in zones if z.kind == "demand"]

    check("4H/1H candle series built", len(htf) > 200 and len(mtf) > 200,
          f"{len(htf)} x 4H, {len(mtf)} x 1H")
    ok = check("graded zones detected", bool(zones), f"{len(zones)} zones")
    if not ok or not demand:
        return False, None

    best = max(demand, key=lambda z: z.score)
    check("demand zone found near the engineered support", abs(best.mid - 100.2) < 1.5,
          f"{best.low:.2f}-{best.high:.2f}")
    check("zone graded A or A+", best.grade in ("A", "A+"),
          f"{best.grade} {best.score}/100 {best.breakdown}")
    check("HTF confluence scored", best.breakdown.get("htf_confluence", 0) >= 20,
          str(best.breakdown.get("htf_confluence")))
    check("liquidity scored", best.breakdown.get("liquidity", 0) >= 10,
          str(best.breakdown.get("liquidity")))
    check("score never exceeds 100", best.score <= 100, str(best.score))
    check("structural label attached to the zone",
          bool(best.flags.get("structural")) or best.breakdown.get("liquidity", 0) >= 10,
          f"label={best.flags.get('structural')}")
    check("4H trend classified", htf_trend(htf)["state"] in
          ("strong_up", "up", "range", "down", "strong_down"), str(htf_trend(htf)["state"]))
    return True, best


def test_structure(zone) -> bool:
    slow = build_ltf(zone, 5 * 60_000, 90)
    fast = build_ltf(zone, 3 * 60_000, 120)
    a = atr(slow, 14)
    liq = LiquidityMap(slow, 2, 2, SETTINGS.equal_level_atr_tol)
    sweep = detect_sweep(slow, "LONG", SETTINGS, zone=zone, atr_val=a, liquidity=liq)
    ok = check("liquidity sweep detected on 5m", bool(sweep.get("found")),
               f"level {sweep.get('level', 0):.3f}, extreme {sweep.get('extreme', 0):.3f}")
    ok &= check("swept level reclaimed", bool(sweep.get("reclaimed")))
    ok &= check("sweep is recent (within the strict window)",
                int(sweep.get("age_bars", 99)) <= SETTINGS.sweep_max_age_bars,
                f"{sweep.get('age_bars')} bars ago, max {SETTINGS.sweep_max_age_bars}")
    ok &= check("sweep took out a structural level",
                bool(sweep.get("structural")),
                str((sweep.get("structural") or {}).get("label")))
    mss = detect_mss(fast, "LONG", SETTINGS)
    ok &= check("market structure shift on 3m", bool(mss.get("found")),
                f"broke {mss.get('level', 0):.3f}")
    return ok


def test_cvd(zone) -> bool:
    micro = build_micro_cvd(zone)
    div = cvd_divergence(micro, "LONG", lookback=90, pivot=2)
    return check("bullish CVD divergence detected", bool(div.get("found")),
                 f"strength {div.get('strength', 0):.2f}")


def test_orderflow(zone) -> Tuple[bool, ArmedContext]:
    ctx = ArmedContext("TESTUSDT", zone, SETTINGS, tick_size=0.01, decimals=2,
                       ref_price=zone.mid)
    # fixed, bucket-aligned anchor: the test must not depend on the wall clock
    now = 1_700_003_600_000
    trades = build_agg_trades(zone, now)
    ctx.book.seed_from_events(trades)
    for t in trades:
        ctx.heatmap.note_execution(t.price, t.qty, t.buy)
    feed_depth(ctx, zone, now)
    ctx.candles[SETTINGS.micro_interval] = build_micro_cvd(zone)
    ctx.candles[SETTINGS.ltf_fast] = build_ltf(zone, 3 * 60_000, 120)
    ctx.candles[SETTINGS.ltf_slow] = build_ltf(zone, 5 * 60_000, 90)
    ctx.candles[SETTINGS.ltf_mid] = build_ltf(zone, 15 * 60_000, 90)
    ctx.armed_at = time.time() - 600
    ctx.seeded = True
    ctx.last_event_ts = time.time()      # the fixture stands in for a live feed
    ctx.last_flow_ts = time.time()
    ctx.refresh_derived(force=True)

    health = ctx.book.health(SETTINGS.min_trades_for_flow)
    check("footprint populated", health["enough"],
          f"{health['trades']} trades in {health['buckets']} buckets")

    absorb = ctx.book.absorption("LONG", SETTINGS.absorption_vol_mult,
                                 SETTINGS.absorption_efficiency, 60 * 45)
    check("absorption detected at the lows", absorb["found"],
          f"{absorb['ratio']}x avg level, {int(absorb['share']*100)}% aggressive sells, "
          f"recovery {absorb['recovery']}")

    dex = ctx.book.delta_extreme("LONG", SETTINGS.delta_extreme_z, 60 * 45)
    check("negative delta extreme detected", dex["found"], f"z={dex['z']}")

    imb = ctx.book.imbalances(SETTINGS.imbalance_ratio, 60 * 45)
    check("stacked buy imbalances in the reclaim", imb["buy_stack"] >= 2,
          f"stack {imb['buy_stack']}, count {imb['buy_count']}")

    ob = ctx.depth.analyse("LONG", zone.mid, SETTINGS)
    check("order book wall consumed or refilled, not pulled (case B)",
          ob["walls"]["case_b"] and not ob["liquidity_pulling"],
          f"{ob['walls']['biggest']}")
    return True, ctx


def test_decision(ctx: ArmedContext) -> Tuple[bool, object]:
    decision = evaluate(ctx, SETTINGS, trend_state="range")
    ok = check("confirmation engine approves the setup", decision.passed,
               f"confidence {decision.confidence}, blockers {decision.blockers}")
    check("multiple confluences recorded", len(decision.reasons) >= 4,
          f"{len(decision.reasons)} reasons")

    signal = build_signal(ctx, decision, SETTINGS, opposing_level=None) if decision.passed else None
    if signal is None:
        check("signal levels built", False, str(decision.blockers))
        return False, None

    check("signal levels built",
          signal.sl < signal.entry_low < signal.entry_high < signal.tp1 < signal.tp2 < signal.tp3,
          f"SL {signal.sl:.2f} | entry {signal.entry_low:.2f}-{signal.entry_high:.2f} "
          f"| TP {signal.tp1:.2f}/{signal.tp2:.2f}/{signal.tp3:.2f}")
    check("risk within bounds",
          SETTINGS.min_risk_pct <= signal.risk_pct <= SETTINGS.max_risk_pct,
          f"{signal.risk_pct*100:.2f}%")
    check("reward-to-risk acceptable", signal.rr >= SETTINGS.min_rr_after_cap, f"{signal.rr:.2f}")

    # a setup fighting a strong opposing trend must be refused
    blocked = evaluate(ctx, SETTINGS, trend_state="strong_down")
    check("counter-trend setup rejected", not blocked.passed and
          "against_4h_trend" in blocked.blockers)

    # a frozen feed must never produce a signal, however good the data looks
    fresh_ts = ctx.last_flow_ts
    ctx.last_flow_ts = time.time() - (SETTINGS.max_flow_age_sec + 60)
    stale = evaluate(ctx, SETTINGS, trend_state="range")
    check("stale order-flow feed rejected", not stale.passed and
          any(b.startswith("stale_flow") for b in stale.blockers), str(stale.blockers))
    ctx.last_flow_ts = fresh_ts
    return ok, signal


async def _test_db(signal) -> bool:
    path = os.path.join(tempfile.mkdtemp(prefix="sniper-selftest-"), "test.db")
    db = Database(path)
    await db.init()
    sid = await db.insert_signal(signal)
    ok = check("signal persisted to sqlite", sid > 0, f"id {sid}")

    events = []

    async def notify(trade, kind, info):
        events.append((kind, round(info.get("r", 0), 2)))

    released = []

    async def release(symbol):
        released.append(symbol)

    monitor = TradeMonitor(db, SETTINGS, notify, release)
    await monitor.load()
    ok &= check("monitor picked up the open setup", monitor.count == 1)

    await monitor.on_prices({signal.symbol: signal.tp1 + 0.001})
    ok &= check("TP1 alert fired and stop moved to breakeven",
                any(e[0] == "TP1" for e in events))

    await monitor.on_prices({signal.symbol: signal.tp2 + 0.001})
    ok &= check("TP2 alert fired", any(e[0] == "TP2" for e in events))

    await monitor.on_prices({signal.symbol: signal.tp3 + 0.001})
    ok &= check("TP3 closed the setup as a win", any(e[0] == "WIN" for e in events))
    ok &= check("symbol released back to the watchlist pool", signal.symbol in released)

    perf = await db.performance(0)
    ok &= check("performance report computed",
                perf["closed"] == 1 and perf["total_r"] >= SETTINGS.tp3_min_r * 0.9,
                f"{perf['closed']} closed, {perf['total_r']:+.2f}R, winrate {perf['winrate']:.0f}%")

    # stop-loss path on a fresh row
    signal.symbol = "LOSSUSDT"
    sid2 = await db.insert_signal(signal)
    row = await db.signal(sid2)
    await monitor.add(row)
    await monitor.on_prices({"LOSSUSDT": signal.sl - 0.001})
    ok &= check("stop loss path closes with a negative R",
                any(e[0] == "LOSS" and e[1] < 0 for e in events), str(events))

    await db.close()
    return ok


async def _swallow(coro):
    try:
        await coro
    except Exception:
        pass


async def _test_resilience(zone) -> bool:
    """
    Regressions for the failure modes that silently killed a live deployment:
    a latched rate limiter, a websocket that connects and then says nothing,
    and a command handler that blocks the Telegram poll loop.
    """
    import logging

    from core.rate_limiter import WeightLimiter
    from notifier.telegram import TelegramBot

    # 1. the exchange's reported weight must decay, not latch forever
    lim = WeightLimiter(1100)
    lim.sync_from_header(1097)
    fresh_reading = lim.reported
    lim._reported_at -= 61                       # the same reading, a minute old
    started = time.time()
    await asyncio.wait_for(lim.acquire(10), timeout=3)
    ok = check("stale weight reading decays instead of deadlocking",
               fresh_reading > 1000 and lim.reported == 0 and (time.time() - started) < 1,
               f"fresh {fresh_reading} -> aged {lim.reported}")

    # 2. a fresh reading must still hold the budget closed
    lim2 = WeightLimiter(1100)
    lim2.sync_from_header(1097)
    blocked = False
    try:
        await asyncio.wait_for(lim2.acquire(50), timeout=1.5)
    except asyncio.TimeoutError:
        blocked = True
    ok &= check("fresh weight reading still enforces the budget", blocked)

    # 2b. the exchange header is ground truth: a local sliding-window estimate
    #     must never keep throttling us after the exchange's minute has reset
    lim3 = WeightLimiter(1100)
    burst = time.time()
    for _ in range(1089):
        lim3._events.append((burst, 1))       # the startup zone-rebuild burst
    saturated = lim3.used
    lim3.sync_from_header(1)                  # exchange minute rolled over
    started = time.time()
    await asyncio.wait_for(lim3.acquire(2), timeout=3)
    ok &= check("exchange header overrides a saturated local estimate",
                saturated > 1000 and lim3.used <= 10 and (time.time() - started) < 1,
                f"local {saturated} -> anchored {lim3.used}")

    # 2c. background work must not be able to starve latency-sensitive calls
    lim4 = WeightLimiter(1100)
    for _ in range(lim4.bulk_budget):
        lim4._events.append((time.time(), 1))
    bulk_blocked = False
    try:
        await asyncio.wait_for(lim4.acquire(5, bulk=True), timeout=1)
    except asyncio.TimeoutError:
        bulk_blocked = True
    priority_ok = True
    try:
        await asyncio.wait_for(lim4.acquire(2), timeout=1)
    except asyncio.TimeoutError:
        priority_ok = False
    ok &= check("bulk work throttles while priority calls keep headroom",
                bulk_blocked and priority_ok,
                f"bulk cap {lim4.bulk_budget} of {lim4.budget}")

    # 2d. one corrupt timestamp must not be able to wipe the footprint book
    from core.orderflow import FootprintBook
    fb = FootprintBook(0.01, 100.0, 60, 90, 60)
    base = 1_700_000_000_000
    for i in range(400):
        fb.add(100 + i * 0.001, 2, i % 3 != 0, base + i * 1000)
    intact = fb.total_trades
    fb.add(100.5, 1, True, 1_900_000_000_000)     # clock-skewed far future
    fb.add(100.5, 1, True, 1_700_000_000)         # seconds where ms expected
    fb.add(100.5, 1, True, base + 401_000)        # legitimate next print
    ok &= check("corrupt timestamps rejected without destroying the book",
                intact == 400 and fb.total_trades == 401 and fb.rejected_ts == 2,
                f"{fb.total_trades} trades kept, {fb.rejected_ts} rejected")

    # 2e. the budget must complain quietly, not once per second per task
    lim5 = WeightLimiter(100, name="test")
    for _ in range(100):
        lim5._events.append((time.time(), 1))
    warnings = []

    class _Capture(logging.Handler):
        def emit(self, record):
            warnings.append(record.getMessage())

    rl_log = logging.getLogger("ratelimit")
    handler = _Capture()
    rl_log.addHandler(handler)
    try:
        # five tasks contending, exactly as five zone builds would
        await asyncio.gather(*[
            asyncio.wait_for(_swallow(lim5.acquire(50)), timeout=2) for _ in range(5)
        ], return_exceptions=True)
    finally:
        rl_log.removeHandler(handler)
    ok &= check("budget contention logs at most once, not once per task per second",
                len(warnings) <= 1, f"{len(warnings)} log lines from 5 contending tasks")

    # 2f. a 4xx must not be retried - it burns weight for a guaranteed failure
    from core.exchanges.base import PermanentRequestError
    ok &= check("client errors are a distinct, non-retryable failure",
                issubclass(PermanentRequestError, RuntimeError))

    # 3. a silent socket must be detected rather than treated as healthy
    from core.ws import MarkPriceStream
    st = MarkPriceStream("wss://example.invalid", idle_timeout=2)
    ok &= check("mark price stream reports never-received honestly",
                st.stale_for == float("inf") and not st.healthy
                and st.describe_staleness() == "no data since connect")
    ok &= check("mark price stream has fallback endpoints", len(st.urls) >= 2,
                f"{len(st.urls)} endpoints")

    # 4. REST fallback must revive a context whose websocket died
    ctx = ArmedContext("TESTUSDT", zone, SETTINGS, tick_size=0.01, decimals=2,
                       ref_price=zone.mid)
    ctx.last_flow_ts = time.time() - 900
    stale_before = ctx.flow_age

    class _StubAdapter:
        name = "stub"

        async def recent_trades(self, s, l=1000):
            return build_agg_trades(zone, 1_700_003_600_000)

        async def klines(self, s, i, l, bulk=False):
            return build_ltf(zone, 60_000, 120)

        async def depth(self, s, l=50):
            return DepthEvent("stub", s, [(1.0, 5.0)], [(2.0, 5.0)], 1_700_003_600_000)

    stub = _StubAdapter()
    await ctx.poll_rest(stub, stub)
    ok &= check("REST fallback revives a dead order-flow feed",
                stale_before > 800 and ctx.flow_age < 5 and ctx.book.total_trades > 0,
                f"flow age {stale_before:.0f}s -> {ctx.flow_age:.1f}s, "
                f"{ctx.book.total_trades} trades")

    # 5. a blocking command must not wedge the poll loop
    bot = TelegramBot("token", "1", command_timeout=1)
    delivered = []

    async def _never_returns(cmd, args):
        await asyncio.sleep(30)

    async def _capture(text, **kw):
        delivered.append(text)

    bot.send = _capture
    await asyncio.wait_for(bot._dispatch(_never_returns, "status", []), timeout=5)
    ok &= check("hung command times out instead of silencing the bot",
                bool(delivered) and "timed out" in delivered[0])
    return ok


async def _test_multi_exchange() -> bool:
    """Venue dialects must arrive downstream as one identical event model."""
    from core.exchanges.binance import BinanceStreamSession
    from core.exchanges.bybit import BybitStreamSession
    from core.feed import EventBus, PriceBook, SymbolRouter

    bn = BinanceStreamSession("wss://x", "BTCUSDT", ["1m", "5m"], 20, 500)
    by = BybitStreamSession("wss://y", "BTCUSDT", ["1m", "5m"], 20, 500)

    # Binance flags the MAKER side, Bybit flags the TAKER side. Both of these
    # describe the same event: an aggressive SELL.
    b_ev = bn.handle({"data": {"e": "aggTrade", "p": "100", "q": "2",
                               "m": True, "T": 1, "a": 5}})[0]
    y_ev = by.handle({"topic": "publicTrade.BTCUSDT", "ts": 1,
                      "data": [{"T": 1, "S": "Sell", "v": "2", "p": "100", "i": "5"}]})[0]
    ok = check("opposite venue side conventions normalise identically",
               b_ev.buy is False and y_ev.buy is False and b_ev.qty == y_ev.qty,
               f"binance buy={b_ev.buy}, bybit buy={y_ev.buy}")

    by.handle({"topic": "orderbook.50.BTCUSDT", "type": "snapshot", "ts": 1000,
               "data": {"b": [["99", "5"], ["98", "3"]], "a": [["101", "4"]]}})
    d = by.handle({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 2000,
                   "data": {"b": [["99", "0"], ["97", "9"]], "a": []}})
    ok &= check("bybit delta book reconstructs into full snapshots",
                bool(d) and d[0].bids == [(98.0, 3.0), (97.0, 9.0)],
                str(d[0].bids if d else None))
    ok &= check("bybit depth stream is throttled",
                by.handle({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 2100,
                           "data": {"b": [["96", "1"]], "a": []}}) == [])

    k = by.handle({"topic": "kline.5.BTCUSDT", "ts": 1, "data": [
        {"start": "1000", "end": "2000", "interval": "5", "open": "1", "high": "2",
         "low": "0.5", "close": "1.5", "volume": "10", "turnover": "15", "confirm": True}]})
    ok &= check("bybit intervals map to canonical notation",
                k[0].interval == "5m" and k[0].closed, k[0].interval)

    # 50/50 split, honouring single-venue listings
    router = SymbolRouter(SETTINGS)
    common = {f"C{i}" for i in range(100)}
    listings = {"binance": common | {"ONLYB"}, "bybit": common | {"ONLYY"}}
    plan = router.plan([f"C{i}" for i in range(100)] + ["ONLYB", "ONLYY"],
                       listings, ["binance", "bybit"])
    counts = router.summary()
    ok &= check("watchlist splits evenly across venues",
                abs(counts["binance"] - counts["bybit"]) <= 1,
                f"{counts}")
    ok &= check("single-venue symbols route to the only venue that lists them",
                plan["ONLYB"] == "binance" and plan["ONLYY"] == "bybit")

    # the queue: sockets never block, newest data wins under pressure
    bus = EventBus(shards=2, maxsize=4, batch=32)
    for i in range(10):
        bus.publish(TradeEvent("binance", "BTCUSDT", 100 + i, 1, True, 1000 + i, i))
    stats = bus.health()
    seen = []

    async def consume(ev):
        seen.append(ev)

    bus.start(consume)
    await asyncio.sleep(0.2)
    await bus.stop()
    ok &= check("queue sheds oldest events instead of blocking the socket",
                stats["dropped"] > 0 and seen and seen[-1].tid == 9,
                f"dropped {stats['dropped']}, newest kept tid={seen[-1].tid if seen else None}")

    book = PriceBook()
    book.bulk_update({"BTCUSDT": 100.0}, "binance-ws")
    book.update("BTCUSDT", 101.0, "bybit-trade")
    ok &= check("price book merges venues, newest wins",
                book.get("BTCUSDT") == 101.0 and book.sources["BTCUSDT"] == "bybit-trade")
    return ok


def test_institutional(ctx, zone) -> bool:
    """Heatmap, execution algorithms and the liquidity-first gate."""
    heat = ctx.heatmap
    summary = heat.summary(ctx.price)
    ok = check("heatmap accumulated resting liquidity",
               summary["buckets"] > 0 and summary["updates"] > 0,
               f"{summary['buckets']} buckets from {summary['updates']} snapshots")

    targets = heat.target_pools("LONG", ctx.price, 0.2)
    ok &= check("heatmap exposes an upside liquidity pool as a target",
                bool(targets), f"{[round(t.price, 2) for t in targets[:3]]}")

    support = heat.support_pool("LONG", ctx.price, 0.02)
    ok &= check("heatmap identifies the pool defending the level",
                support is not None and support.verdict in ("iceberg", "consumed", "resting"),
                f"{support.to_dict() if support else None}")

    trades = ctx.book.recent_trades(SETTINGS.algo_window_sec)
    slicing = detect_slicing(trades, min_clips=SETTINGS.algo_min_clips,
                             regularity_max=SETTINGS.algo_regularity_max)
    ok &= check("uniform clips at regular intervals flagged as TWAP slicing",
                slicing["found"] and slicing["twap"],
                f"clip {slicing['clip']} x{slicing['count']} every {slicing['period']}s, "
                f"dispersion {slicing['regularity']}")

    ice = detect_iceberg(trades, heat, ctx.book.grid, min_ratio=SETTINGS.iceberg_ratio)
    ok &= check("executed volume far above displayed size flagged as iceberg",
                ice["found"], f"{ice['executed']} traded vs {ice['displayed']} shown "
                              f"({ice['ratio']}x) on the {ice['side']}")

    liq = LiquidityMap(ctx.candles[SETTINGS.ltf_mid], 2, 2, SETTINGS.equal_level_atr_tol)
    labels = {l.label for l in liq.highs + liq.lows}
    ok &= check("swings labelled as HH/HL/LH/LL", bool(labels & {"HH", "HL", "LH", "LL"}),
                str(sorted(labels)))

    tgts = liquidity_targets(ctx, "LONG", ctx.price)
    ok &= check("liquidity targets built from heatmap and structure",
                bool(tgts) and len({t["source"] for t in tgts}) >= 1,
                f"{len(tgts)} targets, sources {sorted({t['source'] for t in tgts})}")

    v = session_vwap(ctx.candles[SETTINGS.micro_interval])
    ok &= check("session VWAP computed with bands",
                bool(v) and v["upper"] > v["vwap"] > v["lower"],
                f"vwap {v.get('vwap', 0):.2f}")
    return ok


def test_signal_health(ctx, zone) -> bool:
    """
    The gates must reject bad setups without strangling good ones. Each of
    these was a live bug that silently suppressed valid signals.
    """
    from core.orderflow import FootprintBook

    # 1. absorption measured over the retention window instead of a recent one
    b = FootprintBook(0.01, 100.0, 60, 90, 60)
    ts = 1_700_000_000_000

    def push(p, q, buy, gap=300):
        nonlocal ts
        b.add(p, q, buy, ts)
        ts += gap

    for i in range(700):                       # an hour of unrelated drift
        push(105 - i * 0.0057, random.uniform(1, 3), i % 3 != 0, 5000)
    for i in range(200):                       # old heavy selling, long gone
        push(101 + random.choice([0, 0.01, 0.02]), random.uniform(8, 15), False, 1500)
    for i in range(300):                       # the sweep we actually care about
        push(99.90 + random.choice([0, 0.01, 0.02, 0.03]), random.uniform(9, 18), False, 1200)
    price = 99.95
    while price < 100.6:                       # absorbed and reclaimed
        for _ in range(8):
            push(price, random.uniform(4, 9), True, 400)
        price += 0.05

    wide = b.absorption("LONG", SETTINGS.absorption_vol_mult,
                        SETTINGS.absorption_efficiency, 90 * 60)
    # the window a sweep 8 minutes ago would produce
    anchored = b.absorption("LONG", SETTINGS.absorption_vol_mult,
                            SETTINGS.absorption_efficiency, 8 * 60)
    ok = check("absorption anchored to the sweep is not diluted by old price action",
               anchored["found"] and not wide["found"]
               and anchored["recovery"] > wide["recovery"] * 2,
               f"anchored: found (recovery {anchored['recovery']}) · "
               f"90min retention: missed (recovery {wide['recovery']})")
    ok &= check("the absorption window sizes itself to the sweep",
                0 < (evaluate(ctx, SETTINGS, "range").details.get("absorption_window_sec") or 0)
                <= SETTINGS.flow_analysis_min * 60,
                f"{evaluate(ctx, SETTINGS, 'range').details.get('absorption_window_sec')}s "
                f"(cap {SETTINGS.flow_analysis_min * 60}s)")

    # 2. the sweep must be matched against structure from its own timeframe
    maps = {tf: ctx.liquidity_for(tf) for tf in
            (SETTINGS.ltf_fast, SETTINGS.ltf_slow, SETTINGS.ltf_mid)}
    ok &= check("a structural map exists per timeframe",
                all(m is not None for m in maps.values())
                and maps[SETTINGS.ltf_fast] is not maps[SETTINGS.ltf_mid],
                f"{len(set(id(m) for m in maps.values()))} distinct maps")

    decision = evaluate(ctx, SETTINGS, "range")
    ok &= check("sweep resolves to a structural level on its own timeframe",
                bool((decision.details.get("sweep") or {}).get("structural")))

    # 3. the counter-trend cost must be charged once, not twice
    with_trend = evaluate(ctx, SETTINGS, "range")
    against = evaluate(ctx, SETTINGS, "down")
    ok &= check("counter-trend cost is charged once, not against score and bar both",
                abs(with_trend.confidence - against.confidence) < 1e-6,
                f"range {with_trend.confidence} vs down {against.confidence}")

    # 4. a fully evidenced counter-trend reversal must be reachable
    original = SETTINGS.sweep_fresh_bars
    try:
        SETTINGS.sweep_fresh_bars = 5
        fresh = evaluate(ctx, SETTINGS, "down")
        ok &= check("counter-trend passes when the evidence is genuinely there",
                    fresh.passed, str(fresh.blockers[:2]))
    finally:
        SETTINGS.sweep_fresh_bars = original

    stale = evaluate(ctx, SETTINGS, "down")
    ok &= check("counter-trend without a fresh sweep is still refused",
                not stale.passed and any("counter_trend" in b for b in stale.blockers),
                str(stale.blockers[:1]))
    hard = evaluate(ctx, SETTINGS, "strong_down")
    ok &= check("a strong opposing trend is refused outright",
                not hard.passed and "against_4h_trend" in hard.blockers)

    # 5. lower-timeframe bias must cost confidence, not veto the trade
    ok &= check("opposed lower timeframes no longer hard-block a reversal",
                "all_lower_timeframes_opposed" not in with_trend.blockers)
    return ok


def test_liquidity_gate(ctx) -> bool:
    """A setup with nowhere to go must not fire, however clean the flow is."""
    heat_bids, heat_asks = dict(ctx.heatmap.bids), dict(ctx.heatmap.asks)
    ltf, htf = ctx.ltf_liquidity, ctx.htf_liquidity
    try:
        ctx.heatmap.asks.clear()          # remove every upside pool
        ctx.ltf_liquidity = None
        ctx.htf_liquidity = None
        blocked = evaluate(ctx, SETTINGS, trend_state="range")
        ok = check("setup with no liquidity target is rejected",
                   not blocked.passed and "no_liquidity_target" in blocked.blockers,
                   str(blocked.blockers[:3]))
    finally:
        ctx.heatmap.bids, ctx.heatmap.asks = heat_bids, heat_asks
        ctx.ltf_liquidity, ctx.htf_liquidity = ltf, htf

    stale = dict(ctx.execution)
    sweep_cfg = SETTINGS.sweep_max_age_bars
    try:
        SETTINGS.sweep_max_age_bars = 0    # nothing can be recent enough
        aged = evaluate(ctx, SETTINGS, trend_state="range")
        ok &= check("stale sweep is rejected even with perfect order flow",
                    not aged.passed and any("sweep" in b for b in aged.blockers),
                    str(aged.blockers[:3]))
    finally:
        SETTINGS.sweep_max_age_bars = sweep_cfg
        ctx.execution = stale
    return ok


def test_targets_and_schedule(ctx, zone) -> bool:
    """
    Target sanity and the weekend schedule.

    The 119R target was real: TP3 was offered at a price the market would have
    had to nearly triple to reach, because a distant 4H zone cleared the
    minimum reward and nothing capped the maximum.
    """
    from datetime import datetime, timezone

    from core.engine import SniperEngine

    decision = evaluate(ctx, SETTINGS, "range")
    # a wildly distant "opposing zone", exactly as the live signal had
    absurd = ctx.price * 3.0
    signal = build_signal(ctx, decision, SETTINGS, opposing_level=absurd)
    ok = check("a target three times the price is not offered", signal is not None
               and signal.tp3 < ctx.price * 1.5,
               f"TP3 {signal.tp3:.2f} vs the {absurd:.2f} zone that was offered")

    risk = abs(signal.entry_ref - signal.sl)
    r = [abs(t - signal.entry_ref) / risk for t in (signal.tp1, signal.tp2, signal.tp3)]
    ok &= check("targets are ordered and separated",
                r[0] < r[1] < r[2] and (r[1] - r[0]) >= SETTINGS.tp_min_separation_r * 0.99,
                f"{r[0]:.2f}R / {r[1]:.2f}R / {r[2]:.2f}R")
    ok &= check("TP1 stays close enough to be reachable",
                SETTINGS.tp1_min_r * 0.95 <= r[0] <= SETTINGS.tp1_max_r * 1.05,
                f"TP1 {r[0]:.2f}R (window {SETTINGS.tp1_min_r}-{SETTINGS.tp1_max_r}R)")
    ok &= check("TP3 stays inside the volatility reach",
                abs(signal.tp3 - signal.entry_ref) <= float(signal.meta["reach_cap"]) * 1.01,
                f"{abs(signal.tp3 - signal.entry_ref) / float(signal.meta['reach_cap']) * 100:.0f}% "
                f"of reach")

    engine = SniperEngine(SETTINGS)
    cases = [("Friday 23:00", datetime(2026, 8, 28, 23, 0, tzinfo=timezone.utc), False),
             ("Saturday 10:00", datetime(2026, 8, 29, 10, 0, tzinfo=timezone.utc), True),
             ("Sunday 20:00", datetime(2026, 8, 30, 20, 0, tzinfo=timezone.utc), True),
             ("Monday 05:30", datetime(2026, 8, 31, 5, 30, tzinfo=timezone.utc), True),
             ("Monday 06:30", datetime(2026, 8, 31, 6, 30, tzinfo=timezone.utc), False)]
    results = []
    for _, when, expected in cases:
        engine.local_now = lambda d=when: d
        results.append(engine.sleep_state()[0] == expected)
    ok &= check("engine sleeps Saturday and Sunday, wakes Monday morning",
                all(results), f"{sum(results)}/{len(cases)} times correct")

    engine.local_now = lambda: datetime(2026, 8, 31, 5, 30, tzinfo=timezone.utc)
    ok &= check("wake time is the coming morning, not the next one",
                engine.next_wake().strftime("%a %H:%M") == "Mon 06:00",
                engine.next_wake().strftime("%a %H:%M"))

    # metals are tradable again; tokenised equities are not
    from core.watchlist import WatchlistManager
    w = WatchlistManager({}, None, SETTINGS)
    w.listings = {SETTINGS.history_exchange: {"XAUTUSDT", "PAXGUSDT", "XAGUSDT",
                                              "MRVLUSDT", "SPCXUSDT"}}
    ok &= check("tokenised gold and silver are tradable",
                not any(w._acceptable(s) for s in ("XAUTUSDT", "PAXGUSDT", "XAGUSDT")))
    ok &= check("tokenised equities are still excluded",
                all(w._acceptable(s) for s in ("MRVLUSDT", "SPCXUSDT")))
    return ok


def test_universe() -> bool:
    """Junk listings must never reach the strategy."""
    from core.watchlist import WatchlistManager
    w = WatchlistManager({}, None, SETTINGS)
    w.listings = {"binance": {"BTCUSDT", "ETHUSDT", "TRUMPUSDT"},
                  "bybit": {"BTCUSDT", "MRVLUSDT", "SPCXUSDT", "XAGUSDT",
                            "\u6211\u8e0f\u9a6c\u6765\u4e86USDT", "MUUSDT"}}
    accepted = [s for s in ("BTCUSDT", "ETHUSDT", "TRUMPUSDT") if not w._acceptable(s)]
    ok = check("ordinary crypto perpetuals are accepted", len(accepted) == 3, str(accepted))
    ok &= check("tokenised equities and metals are rejected",
                all(w._acceptable(s) for s in ("MRVLUSDT", "SPCXUSDT", "XAGUSDT")),
                w._acceptable("MRVLUSDT"))
    ok &= check("non-ASCII tickers are rejected",
                bool(w._acceptable("\u6211\u8e0f\u9a6c\u6765\u4e86USDT")),
                w._acceptable("\u6211\u8e0f\u9a6c\u6765\u4e86USDT"))
    ok &= check("symbols without candle history are rejected",
                bool(w._acceptable("MUUSDT")), w._acceptable("MUUSDT"))
    return ok


async def _test_starvation(zone) -> bool:
    """
    The exact failure seen live: a stream that delivers depth and klines but
    no trades, so the context starves while looking perfectly connected, and
    the same symbols re-arm on the same broken venue every twelve minutes.
    """
    import os
    import tempfile

    from core.engine import SniperEngine

    original_db = SETTINGS.db_path
    original = (SETTINGS.flow_poll_sec, SETTINGS.ws_flow_idle_timeout_sec,
                SETTINGS.max_flow_age_sec)
    SETTINGS.db_path = os.path.join(tempfile.mkdtemp(prefix="sniper-starve-"), "s.db")
    SETTINGS.flow_poll_sec, SETTINGS.ws_flow_idle_timeout_sec = 1, 2
    SETTINGS.max_flow_age_sec = 3
    polled: List[str] = []

    class _Adapter:
        name = "binance"
        has_taker_volume = True

        async def start(self):
            return None

        async def close(self):
            return None

        async def recent_trades(self, s, l=1000):
            polled.append(s)
            return []

        async def klines(self, s, i, l, bulk=False):
            return build_ltf(zone, 60_000, 120)

        async def depth(self, s, l=50):
            return DepthEvent("binance", s, [(1.0, 5.0)], [(2.0, 5.0)],
                              int(time.time() * 1000))

    engine = SniperEngine(SETTINGS)
    engine.adapters = {"binance": _Adapter(), "bybit": _Adapter()}
    engine.history = engine.adapters["binance"]
    await engine.db.init()
    try:
        ctx = ArmedContext("BLESSUSDT", zone, SETTINGS, 0.01, 2, zone.mid, "binance")
        ctx.seeded = True
        ctx.armed_at = time.time() - 300
        ctx.last_flow_ts = time.time() - 60          # trades stopped a minute ago
        engine.armed["BLESSUSDT"] = ctx
        task = asyncio.create_task(engine._loop_flow_fallback())

        # the socket is alive - depth keeps arriving, trades never do
        for _ in range(4):
            ctx.on_depth(DepthEvent("binance", "BLESSUSDT", [(1.0, 5.0)],
                                    [(2.0, 5.0)], int(time.time() * 1000)))
            await asyncio.sleep(0.6)
        task.cancel()

        ok = check("a stream with depth but no trades still triggers the REST rescue",
                   bool(polled) and ctx.flow_age < 5,
                   f"{len(polled)} rescue polls, flow age {ctx.flow_age:.1f}s")

        # a venue that starves its symbols while the other works gets bypassed
        engine.venue_trades["bybit"] = 500
        engine.watchlist.symbols = ["BLESSUSDT"]
        engine.watchlist.listings = {"binance": {"BLESSUSDT"}, "bybit": {"BLESSUSDT"}}
        for i in range(SETTINGS.venue_starve_threshold):
            engine._note_starvation(f"SYM{i}USDT", "binance")
        ok &= check("a venue that delivers no trades is dropped from streaming",
                    "binance" not in engine.active_venues()
                    and "bybit" in engine.active_venues(),
                    f"active: {engine.active_venues()}")
        ok &= check("candle history still uses the blocked venue over REST",
                    engine.history.name == "binance")

        engine.starve_count["BLESSUSDT"] = 3
        penalty = min(3600, 600 * (2 ** min(engine.starve_count["BLESSUSDT"] - 1, 3)))
        ok &= check("repeat starvation backs off instead of looping every 12 minutes",
                    penalty >= 2400, f"{penalty//60} minute cooldown on the 3rd failure")
    finally:
        (SETTINGS.flow_poll_sec, SETTINGS.ws_flow_idle_timeout_sec,
         SETTINGS.max_flow_age_sec) = original
        SETTINGS.db_path = original_db
        await engine.db.close()
    return ok


def test_formatting(signal) -> bool:
    from notifier import formatter as fmt
    msg = fmt.signal_message(signal, 1, " · fresh")
    ok = check("telegram signal card renders", "SNIPER SIGNAL" in msg and "TP3" in msg,
               f"{len(msg)} chars")
    ok &= check("help card renders", "/status" in fmt.help_message())
    return ok


# ---------------------------------------------------------------------- runner
def run_selftest() -> bool:
    print("\n\033[1mSNIPER FLOW - offline self-test\033[0m")
    print("Synthetic market -> zones -> order flow -> decision -> signal -> monitor\n")

    print("\033[1m1. Support / resistance scoring\033[0m")
    ok, zone = test_zones()
    if not ok or zone is None:
        _summary()
        return False

    print("\n\033[1m2. Market structure\033[0m")
    test_structure(zone)

    print("\n\033[1m3. CVD divergence\033[0m")
    test_cvd(zone)

    print("\n\033[1m4. Order flow (footprint / absorption / book)\033[0m")
    _, ctx = test_orderflow(zone)

    print("\n\033[1m5. Institutional analysis (heatmap / algos / liquidity)\033[0m")
    test_institutional(ctx, zone)

    print("\n\033[1m6. Confirmation and risk model\033[0m")
    ok, signal = test_decision(ctx)
    if signal is None:
        _summary()
        return False

    test_liquidity_gate(ctx)

    print("\n\033[1m7. Signal health (windows, timeframes, trend gating)\033[0m")
    test_signal_health(ctx, zone)

    print("\n\033[1m8. Database and trade monitor\033[0m")
    asyncio.run(_test_db(signal))

    print("\n\033[1m9. Multi-exchange feed\033[0m")
    asyncio.run(_test_multi_exchange())

    print("\n\033[1m10. Feed starvation and venue failover\033[0m")
    asyncio.run(_test_starvation(zone))

    print("\n\033[1m11. Universe filtering\033[0m")
    test_universe()

    print("\n\033[1m12. Targets and schedule\033[0m")
    test_targets_and_schedule(ctx, zone)

    print("\n\033[1m13. Telegram rendering\033[0m")
    test_formatting(signal)

    print("\n\033[1m14. Resilience (rate limiter, dead feeds, hung commands)\033[0m")
    asyncio.run(_test_resilience(zone))

    return _summary()


def _summary() -> bool:
    passed = len([r for r in _results if r[1]])
    total = len(_results)
    print(f"\n\033[1m{passed}/{total} checks passed\033[0m")
    failed = [r[0] for r in _results if not r[1]]
    if failed:
        print("Failed: " + ", ".join(failed))
    else:
        print("Every stage of the pipeline behaves as designed.\n")
    return passed == total

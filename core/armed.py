"""
Per-symbol live context, created when price enters a graded zone and destroyed
when it leaves, fires, or goes stale.

This object owns everything expensive: the footprint book, the DOM tracker,
the liquidity heatmap, and the lower-timeframe candle series. Holding that
state only for armed symbols is what keeps a 200-symbol watchlist cheap.

It never computes anything in a socket callback. Events arrive here from the
queue worker, ingestion is O(1) per event, and the heavier derived views
(liquidity maps, execution-algorithm analysis) are recomputed on a timer.
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional

from .events import DepthEvent, KlineEvent, TradeEvent
from .execution_algos import analyse_execution, vwap_context
from .heatmap import LiquidityHeatmap
from .liquidity import LiquidityMap
from .models import Candle, Zone
from .orderbook import DepthTracker
from .orderflow import FootprintBook
from .utils import get_logger, now_ms, safe_float

log = get_logger("armed")


class ArmedContext:
    def __init__(self, symbol: str, zone: Zone, cfg, tick_size: float,
                 decimals: int, ref_price: float, exchange: str = "binance",
                 htf_liquidity: Optional[LiquidityMap] = None):
        self.symbol = symbol
        self.zone = zone
        self.cfg = cfg
        self.exchange = exchange
        self.direction = zone.direction
        self.tick_size = tick_size
        self.decimals = decimals
        self.armed_at = time.time()
        self.last_eval = 0.0
        self.evaluations = 0
        self.last_price = ref_price
        self.last_blockers: List[str] = []
        self.seeded = False
        self.last_event_ts = 0.0
        self.last_flow_ts = 0.0
        self.rest_polls = 0
        self.events_in = 0
        self._last_agg_id = 0
        self._last_trade_ts = 0

        self.book = FootprintBook(
            tick_size=tick_size, ref_price=ref_price,
            bucket_sec=cfg.footprint_bucket_sec,
            window_min=cfg.footprint_window_min,
            price_bins=cfg.footprint_price_bins,
        )
        self.depth = DepthTracker(wall_mult=cfg.ob_wall_mult, keep=cfg.ob_snapshots_keep)
        self.heatmap = LiquidityHeatmap(
            ref_price=ref_price, tick_size=tick_size,
            buckets=cfg.heatmap_buckets, half_life_sec=cfg.heatmap_half_life_sec,
            range_pct=cfg.heatmap_range_pct,
        )

        self.intervals = [cfg.micro_interval, cfg.ltf_fast, cfg.ltf_slow, cfg.ltf_mid]
        self.candles: Dict[str, List[Candle]] = {iv: [] for iv in self.intervals}
        self._limits = {
            cfg.micro_interval: cfg.micro_limit,
            cfg.ltf_fast: cfg.ltf_limit,
            cfg.ltf_slow: cfg.ltf_limit,
            cfg.ltf_mid: cfg.ltf_limit,
        }

        # derived views, refreshed on a timer rather than per event
        self.htf_liquidity = htf_liquidity
        self.ltf_liquidity: Optional[LiquidityMap] = None
        self._liq_maps: Dict[str, LiquidityMap] = {}
        self.execution: Dict[str, object] = {}
        self.vwap: Dict[str, object] = {}
        self._derived_ts = 0.0

    # ------------------------------------------------------------------- seed
    async def seed(self, flow_adapter, history_adapter) -> None:
        """
        Warm start. Candle history comes from the venue that publishes taker
        volume (Binance), because historical delta and CVD depend on it; the
        live tape and book come from whichever venue was assigned this symbol.
        """
        for interval, limit in self._limits.items():
            try:
                self.candles[interval] = await history_adapter.klines(
                    self.symbol, interval, limit)
            except Exception as exc:  # noqa: BLE001
                log.warning("%s seed klines %s failed: %s", self.symbol, interval, exc)

        try:
            trades = await flow_adapter.recent_trades(self.symbol, 1000)
            self.book.seed_from_events(trades)
            for t in trades:
                self.heatmap.note_execution(t.price, t.qty, t.buy)
            if trades:
                self._remember_trade(trades[-1])
        except Exception as exc:  # noqa: BLE001
            log.warning("%s seed trades failed: %s", self.symbol, exc)

        try:
            snap = await flow_adapter.depth(self.symbol, 50)
            if snap:
                self.depth.update([[p, q] for p, q in snap.bids],
                                  [[p, q] for p, q in snap.asks], snap.ts or now_ms())
                self.heatmap.ingest(snap)
        except Exception as exc:  # noqa: BLE001
            log.debug("%s seed depth failed: %s", self.symbol, exc)

        self.seeded = True
        self.last_flow_ts = time.time()
        self.refresh_derived(force=True)
        log.info("armed %s %s via %s | zone %.*f-%.*f score=%d (%d trades seeded)",
                 self.symbol, self.direction, self.exchange,
                 self.decimals, self.zone.low, self.decimals, self.zone.high,
                 self.zone.score, self.book.total_trades)

    # --------------------------------------------------------------- ingestion
    def on_trade(self, ev: TradeEvent) -> None:
        self.book.add(ev.price, ev.qty, ev.buy, ev.ts)
        self.heatmap.note_execution(ev.price, ev.qty, ev.buy)
        self.last_price = ev.price or self.last_price
        self.last_event_ts = self.last_flow_ts = time.time()
        self.events_in += 1
        self._remember_trade(ev)

    def on_depth(self, ev: DepthEvent) -> None:
        self.depth.update([[p, q] for p, q in ev.bids], [[p, q] for p, q in ev.asks],
                          ev.ts or now_ms())
        self.heatmap.ingest(ev)
        self.last_event_ts = time.time()
        self.events_in += 1

    def on_kline(self, ev: KlineEvent) -> None:
        if ev.interval in self.candles:
            self._upsert(ev.interval, ev.candle)
        self.last_event_ts = time.time()
        self.events_in += 1

    def _upsert(self, interval: str, candle: Candle) -> None:
        series = self.candles[interval]
        if series and series[-1].ts == candle.ts:
            # a venue without taker volume must not erase the value we already
            # have from the history feed
            if candle.taker_buy <= 0 < series[-1].taker_buy:
                candle.taker_buy = series[-1].taker_buy
            series[-1] = candle
        else:
            series.append(candle)
            limit = self._limits.get(interval, 200)
            if len(series) > limit + 40:
                del series[: len(series) - limit]

    def _is_new_trade(self, t: TradeEvent) -> bool:
        if t.tid and self._last_agg_id:
            return t.tid > self._last_agg_id
        if t.tid and not self._last_agg_id:
            return True
        return t.ts > self._last_trade_ts

    def _remember_trade(self, t: TradeEvent) -> None:
        self._last_agg_id = max(self._last_agg_id, int(t.tid or 0))
        self._last_trade_ts = max(self._last_trade_ts, int(t.ts or 0))

    # ------------------------------------------------------------ rest fallback
    async def poll_rest(self, flow_adapter, history_adapter) -> bool:
        """Keep a context alive when its websocket is silent."""
        ok = False
        try:
            trades = await flow_adapter.recent_trades(self.symbol, 1000)
            new = [t for t in trades if self._is_new_trade(t)]
            if new:
                self.book.seed_from_events(new)
                for t in new:
                    self.heatmap.note_execution(t.price, t.qty, t.buy)
                self._remember_trade(new[-1])
                self.last_price = new[-1].price or self.last_price
                ok = True
            self.last_flow_ts = time.time()
        except Exception as exc:  # noqa: BLE001
            log.debug("%s trade poll failed: %s", self.symbol, exc)

        for interval in self.intervals:
            try:
                fresh = await history_adapter.klines(self.symbol, interval, 100)
                for c in fresh[-3:]:
                    self._upsert(interval, c)
            except Exception as exc:  # noqa: BLE001
                log.debug("%s kline poll %s failed: %s", self.symbol, interval, exc)

        try:
            snap = await flow_adapter.depth(self.symbol, 50)
            if snap:
                self.depth.update([[p, q] for p, q in snap.bids],
                                  [[p, q] for p, q in snap.asks], snap.ts or now_ms())
                self.heatmap.ingest(snap)
        except Exception as exc:  # noqa: BLE001
            log.debug("%s depth poll failed: %s", self.symbol, exc)

        self.rest_polls += 1
        return ok

    # ------------------------------------------------------------------ derived
    def refresh_derived(self, force: bool = False) -> None:
        """
        Recompute the expensive views on a timer.

        Rebuilding liquidity maps and re-scanning the tape for execution
        algorithms on every trade event would burn the CPU on a 2-core box for
        no benefit - these change on the scale of minutes, not milliseconds.
        """
        now = time.time()
        if not force and (now - self._derived_ts) < self.cfg.derived_refresh_sec:
            return
        self._derived_ts = now

        # one map per timeframe: whatever finds a sweep must be matched against
        # structure from the same candles, or the levels never line up
        for interval in (self.cfg.ltf_fast, self.cfg.ltf_slow, self.cfg.ltf_mid):
            series = self.closed_candles(interval)
            if len(series) >= 40:
                self._liq_maps[interval] = LiquidityMap(
                    series, left=2, right=2, equal_tol_atr=self.cfg.equal_level_atr_tol)
        self.ltf_liquidity = (self._liq_maps.get(self.cfg.ltf_mid)
                              or self._liq_maps.get(self.cfg.ltf_slow))
        try:
            self.execution = analyse_execution(self.book, self.heatmap, self.direction,
                                               window_sec=self.cfg.algo_window_sec,
                                               cfg=self.cfg)
        except Exception as exc:  # noqa: BLE001
            log.debug("%s execution analysis failed: %s", self.symbol, exc)
            self.execution = {}
        micro = self.candles.get(self.cfg.micro_interval) or []
        if micro:
            self.vwap = vwap_context(micro, self.price, self.direction,
                                     lookback=self.cfg.vwap_lookback)

    def closed_candles(self, interval: str) -> List[Candle]:
        """
        Candles that have actually finished.

        Structure must be confirmed on closed bars. A 3m candle thirty seconds
        old can be above a level and back under it before it prints - reading
        the forming bar means the reclaim and the structure shift can both
        evaporate seconds after the signal fires, which is what a cluster of
        stop-outs inside twenty minutes looks like.
        """
        series = self.candles.get(interval) or []
        if not self.cfg.require_closed_candles:
            return series
        if series and series[-1].close_ts and series[-1].close_ts > now_ms():
            return series[:-1]
        return series

    def liquidity_for(self, interval: str) -> Optional[LiquidityMap]:
        """Structural map built from a specific timeframe's candles."""
        return self._liq_maps.get(interval) or self.ltf_liquidity

    def micro_cvd_candles(self) -> List[Candle]:
        """
        Candles for CVD divergence.

        Prefer venue klines carrying taker volume; if the assigned venue does
        not publish it, synthesise minute candles from the live tape instead so
        the analysis still works.
        """
        micro = self.candles.get(self.cfg.micro_interval) or []
        if micro and any(c.taker_buy > 0 for c in micro[-30:]):
            return micro
        return self.book.as_candles()

    # ----------------------------------------------------------------- status
    @property
    def feed_age(self) -> float:
        return (time.time() - self.last_event_ts) if self.last_event_ts else \
            (time.time() - self.armed_at)

    @property
    def flow_age(self) -> float:
        return (time.time() - self.last_flow_ts) if self.last_flow_ts else \
            (time.time() - self.armed_at)

    def flow_fresh(self, max_age: float) -> bool:
        return self.flow_age <= max_age

    @property
    def price(self) -> float:
        micro = self.candles.get(self.cfg.micro_interval) or []
        if self.book.last_ts and self.book.recent:
            return self.book.recent[-1][1]
        return micro[-1].close if micro else self.last_price

    @property
    def age_min(self) -> float:
        return (time.time() - self.armed_at) / 60.0

    @property
    def warm(self) -> bool:
        return (time.time() - self.armed_at) >= self.cfg.arm_warmup_sec and self.seeded

    @property
    def expired(self) -> bool:
        return self.age_min >= self.cfg.arm_ttl_minutes

    def entry_window_ok(self, atr_val: float = 0.0) -> bool:
        """
        Asymmetric validity window: reclaiming *out* of a demand zone is the
        setup, not a reason to bail, but chasing it 3 zone-heights later is.
        """
        z, p = self.zone, self.price
        wrong_side = 0.75 * z.height
        right_side = max(1.8 * z.height, 1.2 * atr_val)
        if self.direction == "LONG":
            return (z.low - wrong_side) <= p <= (z.high + right_side)
        return (z.low - right_side) <= p <= (z.high + wrong_side)

    def still_in_range(self) -> bool:
        z, p = self.zone, self.price
        pad = 1.2 * z.height
        return (z.low - pad) <= p <= (z.high + 2.5 * z.height) if self.direction == "LONG" \
            else (z.low - 2.5 * z.height) <= p <= (z.high + pad)

    def stats(self) -> dict:
        h = self.book.health(self.cfg.min_trades_for_flow)
        ex = self.execution or {}
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "direction": self.direction,
            "zone": f"{self.zone.low:.{self.decimals}f}-{self.zone.high:.{self.decimals}f}",
            "score": self.zone.score,
            "age_min": round(self.age_min, 1),
            "trades": h["trades"],
            "events": self.events_in,
            "depth_updates": self.depth.updates,
            "heatmap_buckets": len(self.heatmap.bids) + len(self.heatmap.asks),
            "evaluations": self.evaluations,
            "feed_age": round(self.feed_age),
            "flow_age": round(self.flow_age),
            "rest_polls": self.rest_polls,
            "institutional": bool(ex.get("institutional")),
            "blockers": self.last_blockers[:4],
        }

    def dispose(self) -> None:
        self.book.clear()
        self.depth.clear()
        self.heatmap.clear()
        for k in self.candles:
            self.candles[k] = []
        self.ltf_liquidity = None
        self._liq_maps.clear()
        self.execution = {}

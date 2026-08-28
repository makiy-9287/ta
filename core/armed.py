"""
Per-symbol live context, created the moment price enters an A/A+ zone and
destroyed the moment it leaves (or the setup fires / expires).

Holding this state only for armed symbols is what keeps the process small:
a hundred watchlist symbols cost a few kilobytes of zone boxes, while the
expensive footprint / depth machinery exists for a handful at a time.
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional

from .models import Candle, Zone
from .orderbook import DepthTracker
from .orderflow import FootprintBook
from .utils import get_logger, now_ms, safe_float

log = get_logger("armed")


class ArmedContext:
    def __init__(self, symbol: str, zone: Zone, cfg, tick_size: float,
                 decimals: int, ref_price: float):
        self.symbol = symbol
        self.zone = zone
        self.cfg = cfg
        self.direction = zone.direction
        self.tick_size = tick_size
        self.decimals = decimals
        self.armed_at = time.time()
        self.last_eval = 0.0
        self.evaluations = 0
        self.last_price = ref_price
        self.last_blockers: List[str] = []
        self.seeded = False
        self.last_event_ts = 0.0        # last live WS payload
        self.last_flow_ts = 0.0         # last time flow data advanced (WS or REST)
        self.rest_polls = 0
        self._last_agg_id = 0
        self._last_trade_ts = 0

        self.book = FootprintBook(
            tick_size=tick_size, ref_price=ref_price,
            bucket_sec=cfg.footprint_bucket_sec,
            window_min=cfg.footprint_window_min,
            price_bins=cfg.footprint_price_bins,
        )
        self.depth = DepthTracker(wall_mult=cfg.ob_wall_mult, keep=cfg.ob_snapshots_keep)
        self.candles: Dict[str, List[Candle]] = {
            cfg.micro_interval: [], cfg.ltf_fast: [], cfg.ltf_slow: [],
        }
        self._limits = {
            cfg.micro_interval: cfg.micro_limit,
            cfg.ltf_fast: cfg.ltf_limit,
            cfg.ltf_slow: cfg.ltf_limit,
        }

    # ------------------------------------------------------------------- seed
    async def seed(self, rest) -> None:
        """Warm start from REST so we are not blind for the first minutes."""
        cfg = self.cfg
        for interval, limit in self._limits.items():
            try:
                self.candles[interval] = await rest.klines(self.symbol, interval, limit)
            except Exception as exc:  # noqa: BLE001
                log.warning("%s seed klines %s failed: %s", self.symbol, interval, exc)

        try:
            trades = await rest.agg_trades(self.symbol, 1000)
            self.book.seed_from_rest(trades)
            if trades:
                self._remember_trade(trades[-1])
        except Exception as exc:  # noqa: BLE001
            log.warning("%s seed aggTrades failed: %s", self.symbol, exc)

        try:
            snap = await rest.depth(self.symbol, 50)
            self.depth.update(snap.get("bids", []), snap.get("asks", []), now_ms())
        except Exception as exc:  # noqa: BLE001
            log.debug("%s seed depth failed: %s", self.symbol, exc)

        self.seeded = True
        self.last_flow_ts = time.time()
        log.info("armed %s %s zone %.6f-%.6f score=%d (seeded %d trades)",
                 self.symbol, self.direction, self.zone.low, self.zone.high,
                 self.zone.score, self.book.total_trades)

    # -------------------------------------------------------------- ws ingest
    async def on_event(self, event: str, data: dict) -> None:
        self.last_event_ts = time.time()
        if event == "aggTrade":
            self.book.add(
                price=safe_float(data.get("p")),
                qty=safe_float(data.get("q")),
                is_buyer_maker=bool(data.get("m")),
                ts=int(data.get("T") or data.get("E") or now_ms()),
            )
            self.last_price = safe_float(data.get("p"), self.last_price)
            self.last_flow_ts = time.time()
        elif event == "depthUpdate" or ("b" in data and "a" in data and "e" not in data):
            self.depth.update(data.get("b", []), data.get("a", []),
                              int(data.get("T") or data.get("E") or now_ms()))
        elif event == "kline":
            k = data.get("k") or {}
            interval = k.get("i")
            if interval in self.candles:
                self._upsert(interval, Candle.from_ws(k))

    def _upsert(self, interval: str, candle: Candle) -> None:
        series = self.candles[interval]
        if series and series[-1].ts == candle.ts:
            series[-1] = candle
        else:
            series.append(candle)
            limit = self._limits.get(interval, 200)
            if len(series) > limit + 30:
                del series[: len(series) - limit]

    # --------------------------------------------------------- rest fallback
    def _is_new_trade(self, t: dict) -> bool:
        """Prefer the aggregate-trade id, fall back to the timestamp when the
        payload has no id (defensive: never re-ingest the same prints twice,
        and never mistake a quiet tape for a dead feed)."""
        tid = int(safe_float(t.get("a"), 0))
        if tid and self._last_agg_id:
            return tid > self._last_agg_id
        if tid and not self._last_agg_id:
            return True
        return int(safe_float(t.get("T"), 0)) > self._last_trade_ts

    def _remember_trade(self, t: dict) -> None:
        self._last_agg_id = max(self._last_agg_id, int(safe_float(t.get("a"), 0)))
        self._last_trade_ts = max(self._last_trade_ts, int(safe_float(t.get("T"), 0)))

    async def poll_rest(self, rest) -> bool:
        """
        Refresh order flow over REST when the WebSocket feed is silent.

        Some networks (and some cloud regions) accept the WS handshake and then
        deliver nothing. Without this the engine would keep evaluating frozen
        seed data and could fire a signal built on order flow an hour old.
        """
        ok = False
        try:
            trades = await rest.agg_trades(self.symbol, 1000)
            new = [t for t in trades if self._is_new_trade(t)]
            if new:
                self.book.seed_from_rest(new)
                self._remember_trade(new[-1])
                self.last_price = safe_float(new[-1].get("p"), self.last_price)
                ok = True
            # a successful poll means our view of the market is current, even
            # if the tape was quiet - otherwise a slow symbol looks "dead" and
            # gets disarmed by the staleness guard while nothing is wrong
            self.last_flow_ts = time.time()
        except Exception as exc:  # noqa: BLE001
            log.debug("%s aggTrade poll failed: %s", self.symbol, exc)

        for interval in list(self.candles.keys()):
            try:
                fresh = await rest.klines(self.symbol, interval, 100)
                for c in fresh[-3:]:
                    self._upsert(interval, c)
            except Exception as exc:  # noqa: BLE001
                log.debug("%s kline poll %s failed: %s", self.symbol, interval, exc)

        try:
            snap = await rest.depth(self.symbol, 20)
            self.depth.update(snap.get("bids", []), snap.get("asks", []), now_ms())
        except Exception as exc:  # noqa: BLE001
            log.debug("%s depth poll failed: %s", self.symbol, exc)

        self.rest_polls += 1
        return ok

    # ----------------------------------------------------------------- status
    @property
    def feed_age(self) -> float:
        """Seconds since the live WebSocket last delivered anything."""
        return (time.time() - self.last_event_ts) if self.last_event_ts else \
            (time.time() - self.armed_at)

    @property
    def flow_age(self) -> float:
        """Seconds since order-flow data last advanced, by any transport."""
        return (time.time() - self.last_flow_ts) if self.last_flow_ts else \
            (time.time() - self.armed_at)

    def flow_fresh(self, max_age: float) -> bool:
        return self.flow_age <= max_age

    @property
    def price(self) -> float:
        micro = self.candles.get(self.cfg.micro_interval) or []
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
        Asymmetric validity window.

        Reclaiming *out* of a demand zone is the setup, not a reason to bail -
        so we allow generous room on the profit side and only invalidate when
        price breaks decisively through the zone the wrong way, or when it has
        run so far that the entry would be a chase.
        """
        z, p = self.zone, self.price
        wrong_side = 0.75 * z.height
        right_side = max(1.8 * z.height, 1.2 * atr_val)
        if self.direction == "LONG":
            return (z.low - wrong_side) <= p <= (z.high + right_side)
        return (z.low - right_side) <= p <= (z.high + wrong_side)

    def still_in_range(self) -> bool:
        """Looser version used to decide whether to keep the streams open."""
        z, p = self.zone, self.price
        pad = max(1.2 * z.height, 0.0)
        return (z.low - pad) <= p <= (z.high + 2.5 * z.height) if self.direction == "LONG" \
            else (z.low - 2.5 * z.height) <= p <= (z.high + pad)

    def stats(self) -> dict:
        h = self.book.health(self.cfg.min_trades_for_flow)
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "zone": f"{self.zone.low:.{self.decimals}f}-{self.zone.high:.{self.decimals}f}",
            "score": self.zone.score,
            "age_min": round(self.age_min, 1),
            "trades": h["trades"],
            "depth_updates": self.depth.updates,
            "feed_age": round(self.feed_age),
            "flow_age": round(self.flow_age),
            "rest_polls": self.rest_polls,
            "evaluations": self.evaluations,
            "blockers": self.last_blockers[:4],
        }

    def dispose(self) -> None:
        self.book.clear()
        self.depth.clear()
        for k in self.candles:
            self.candles[k] = []

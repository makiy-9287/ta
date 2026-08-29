"""
Liquidity heatmap - the Bookmap/Exocharts view, built from DOM snapshots.

A single order book photograph tells you almost nothing: size appears and
vanishes constantly. What matters is liquidity *persistence over time* - the
shelf that sits there minute after minute, refills when hit, and drags price
toward it. That is what a heatmap shows, and it is what this class measures.

For every price bucket we accumulate `size x seconds` (liquidity-seconds), so
a 500-lot resting for two minutes outweighs a 5000-lot flashed for one second.
Old exposure decays with a half-life so the map reflects the live market
rather than the whole session.

Each bucket also records what happened to it:

    consumed  - executions occurred there and the size fell  -> real liquidity
    pulled    - size vanished with no executions             -> spoof
    refilled  - size fell and came back repeatedly           -> iceberg

Targets are placed at the strongest surviving pools, because that is where the
market is actually trying to go.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .events import DepthEvent
from .utils import clamp, get_logger, mean

log = get_logger("heatmap")


@dataclass
class LiquidityPool:
    price: float
    side: str                 # "bid" | "ask"
    strength: float           # 0-1, relative to the strongest pool on the map
    liq_seconds: float
    peak_size: float
    last_size: float
    executed: float = 0.0
    refills: int = 0
    age_sec: float = 0.0
    distance_pct: float = 0.0

    @property
    def verdict(self) -> str:
        if self.refills >= 2 and self.executed > 0:
            return "iceberg"
        if self.executed > 0 and self.last_size < self.peak_size * 0.6:
            return "consumed"
        if self.last_size < self.peak_size * 0.4 and self.executed <= 0:
            return "pulled"
        return "resting"

    def to_dict(self) -> dict:
        return {"price": self.price, "side": self.side,
                "strength": round(self.strength, 3),
                "verdict": self.verdict, "peak": round(self.peak_size, 2),
                "executed": round(self.executed, 2), "refills": self.refills,
                "distance_pct": round(self.distance_pct * 100, 3)}


@dataclass
class _Bucket:
    price: float
    side: str
    liq_seconds: float = 0.0
    peak_size: float = 0.0
    last_size: float = 0.0
    executed: float = 0.0
    refills: int = 0
    first_ts: int = 0
    last_ts: int = 0
    _falling: bool = False


class LiquidityHeatmap:
    def __init__(self, ref_price: float, tick_size: float, buckets: int = 400,
                 half_life_sec: float = 900.0, range_pct: float = 0.035):
        self.tick = max(tick_size, 1e-12)
        self.grid = self._make_grid(ref_price, self.tick)
        self.range_pct = range_pct
        self.half_life = max(60.0, half_life_sec)
        self.max_buckets = buckets
        self.bids: Dict[float, _Bucket] = {}
        self.asks: Dict[float, _Bucket] = {}
        self._last_ts = 0
        self._last_decay = 0
        self.updates = 0

    @staticmethod
    def _make_grid(ref_price: float, tick: float) -> float:
        target = max(ref_price * 0.00035, tick)
        return max(1, round(target / tick)) * tick

    def _snap(self, price: float) -> float:
        return round(math.floor(price / self.grid) * self.grid, 10)

    # ------------------------------------------------------------------ ingest
    def ingest(self, ev: DepthEvent) -> None:
        if not ev.bids or not ev.asks:
            return
        ts = ev.ts or int(time.time() * 1000)
        dt = 0.0 if not self._last_ts else min(5.0, max(0.0, (ts - self._last_ts) / 1000.0))
        self._last_ts = ts
        self.updates += 1
        mid = ev.mid
        if mid <= 0:
            return
        lo, hi = mid * (1 - self.range_pct), mid * (1 + self.range_pct)

        for side, levels, store in (("bid", ev.bids, self.bids), ("ask", ev.asks, self.asks)):
            seen = set()
            for price, size in levels:
                if price < lo or price > hi or size <= 0:
                    continue
                key = self._snap(price)
                seen.add(key)
                b = store.get(key)
                if b is None:
                    b = _Bucket(price=key, side=side, first_ts=ts)
                    store[key] = b
                b.liq_seconds += size * dt
                if size > b.peak_size:
                    # size coming back after a decline is a refill: the
                    # signature of an iceberg working a level
                    if b._falling and size >= b.peak_size * 0.75:
                        b.refills += 1
                        b._falling = False
                    b.peak_size = size
                elif size < b.last_size * 0.6:
                    b._falling = True
                b.last_size = size
                b.last_ts = ts
            for key, b in store.items():
                if key not in seen and b.last_ts < ts:
                    b.last_size = 0.0
                    b._falling = True

        self._decay(ts)
        self._prune(mid)

    def note_execution(self, price: float, qty: float, buy: bool) -> None:
        """Executions tell us whether a level was eaten or merely withdrawn."""
        key = self._snap(price)
        store = self.asks if buy else self.bids
        b = store.get(key)
        if b is not None:
            b.executed += qty

    def _decay(self, ts: int) -> None:
        if not self._last_decay:
            self._last_decay = ts
            return
        elapsed = (ts - self._last_decay) / 1000.0
        if elapsed < 30:
            return
        factor = 0.5 ** (elapsed / self.half_life)
        for store in (self.bids, self.asks):
            for b in store.values():
                b.liq_seconds *= factor
                b.executed *= factor
        self._last_decay = ts

    def _prune(self, mid: float) -> None:
        lo, hi = mid * (1 - self.range_pct * 1.4), mid * (1 + self.range_pct * 1.4)
        for store in (self.bids, self.asks):
            if len(store) <= self.max_buckets:
                dead = [k for k, b in store.items()
                        if (k < lo or k > hi) and b.liq_seconds < 1e-9]
            else:
                ranked = sorted(store.items(), key=lambda kv: kv[1].liq_seconds)
                dead = [k for k, _ in ranked[: len(store) - self.max_buckets]]
                dead += [k for k in store if k < lo or k > hi]
            for k in set(dead):
                store.pop(k, None)

    # ----------------------------------------------------------------- query
    def pools(self, side: Optional[str] = None, price: float = 0.0,
              min_strength: float = 0.25, limit: int = 8) -> List[LiquidityPool]:
        stores = []
        if side in (None, "bid"):
            stores.append(self.bids)
        if side in (None, "ask"):
            stores.append(self.asks)

        everything = [b for s in stores for b in s.values() if b.liq_seconds > 0]
        if not everything:
            return []
        top = max(b.liq_seconds for b in everything) or 1.0
        now = self._last_ts or int(time.time() * 1000)

        out = []
        for b in everything:
            strength = b.liq_seconds / top
            if strength < min_strength:
                continue
            out.append(LiquidityPool(
                price=b.price, side=b.side, strength=strength,
                liq_seconds=b.liq_seconds, peak_size=b.peak_size,
                last_size=b.last_size, executed=b.executed, refills=b.refills,
                age_sec=(now - b.first_ts) / 1000.0,
                distance_pct=abs(b.price - price) / price if price else 0.0,
            ))
        out.sort(key=lambda p: -p.strength)
        return out[:limit]

    def target_pools(self, direction: str, price: float,
                     min_strength: float = 0.30) -> List[LiquidityPool]:
        """
        Where the move is trying to go.

        A long targets resting ASK liquidity above (the sell orders price must
        eat through, and the magnet drawing it up); a short targets resting BID
        liquidity below.
        """
        side = "ask" if direction == "LONG" else "bid"
        pools = self.pools(side=side, price=price, min_strength=min_strength, limit=12)
        if direction == "LONG":
            pools = [p for p in pools if p.price > price]
            pools.sort(key=lambda p: p.price)
        else:
            pools = [p for p in pools if p.price < price]
            pools.sort(key=lambda p: -p.price)
        return pools

    def support_pool(self, direction: str, price: float, max_distance: float = 0.004
                     ) -> Optional[LiquidityPool]:
        """The pool we are relying on to hold - bids under a long."""
        side = "bid" if direction == "LONG" else "ask"
        pools = [p for p in self.pools(side=side, price=price, min_strength=0.2, limit=12)
                 if p.distance_pct <= max_distance]
        if not pools:
            return None
        return max(pools, key=lambda p: p.strength)

    def imbalance(self, price: float, band_pct: float = 0.004) -> float:
        """Resting bid vs ask liquidity-seconds around price. >1 = bid heavy."""
        lo, hi = price * (1 - band_pct), price * (1 + band_pct)
        bid = sum(b.liq_seconds for k, b in self.bids.items() if lo <= k <= hi)
        ask = sum(b.liq_seconds for k, b in self.asks.items() if lo <= k <= hi)
        if ask <= 0:
            return 3.0 if bid > 0 else 1.0
        return clamp(bid / ask, 0.05, 20.0)

    def summary(self, price: float) -> dict:
        best_bid = self.pools("bid", price, 0.3, 1)
        best_ask = self.pools("ask", price, 0.3, 1)
        return {
            "updates": self.updates,
            "buckets": len(self.bids) + len(self.asks),
            "imbalance": round(self.imbalance(price), 2),
            "top_bid": best_bid[0].to_dict() if best_bid else None,
            "top_ask": best_ask[0].to_dict() if best_ask else None,
        }

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()

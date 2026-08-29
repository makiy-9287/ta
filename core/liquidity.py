"""
Structural liquidity map: where the stops are.

Every major swing point is a liquidity pool. Price reversed there, so traders
who bought the low park their stops just under it and breakout traders park
their entries just above it. That resting cluster is *fuel*, and institutions
route price into it deliberately: drive to the level, push slightly beyond to
trigger the cluster, fill the other side of those stops, reverse.

So the labels matter. A prior HH, HL, LL or LH is not just a chart pattern -
it is a known pocket of resting orders. This module finds them, marks which
are still untapped, and answers the question that has to be asked before any
entry:

    where does the money actually rest right now, above us or below us?

Because a sweep is only worth trading when there is a bigger pool on the other
side for price to travel to.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .indicators import atr, swing_points
from .models import Candle
from .utils import get_logger, mean

log = get_logger("liquidity")


@dataclass
class StructuralLevel:
    price: float
    kind: str              # "high" | "low"
    label: str             # HH | HL | LH | LL | H | L
    index: int
    ts: int
    swept: bool = False
    equal_count: int = 1   # how many swings sit at this level (equal highs/lows)
    strength: float = 0.0  # 0-1
    displacement: float = 0.0   # ATR of the move away from it

    @property
    def major(self) -> bool:
        return self.label in ("HH", "HL", "LH", "LL") and self.strength >= 0.5

    def to_dict(self) -> dict:
        return {"price": self.price, "label": self.label, "swept": self.swept,
                "equals": self.equal_count, "strength": round(self.strength, 2)}


def label_swings(candles: Sequence[Candle], left: int = 2, right: int = 2
                 ) -> Tuple[List[StructuralLevel], List[StructuralLevel]]:
    """Label each swing relative to the previous one of the same kind."""
    highs, lows = swing_points(candles, left, right)
    out_h: List[StructuralLevel] = []
    out_l: List[StructuralLevel] = []

    prev = None
    for idx, price in highs:
        label = "H" if prev is None else ("HH" if price > prev else "LH")
        out_h.append(StructuralLevel(price=price, kind="high", label=label,
                                     index=idx, ts=candles[idx].ts))
        prev = price
    prev = None
    for idx, price in lows:
        label = "L" if prev is None else ("HL" if price > prev else "LL")
        out_l.append(StructuralLevel(price=price, kind="low", label=label,
                                     index=idx, ts=candles[idx].ts))
        prev = price
    return out_h, out_l


class LiquidityMap:
    """Structural liquidity for one symbol on one timeframe."""

    def __init__(self, candles: Sequence[Candle], left: int = 2, right: int = 2,
                 equal_tol_atr: float = 0.12):
        self.candles = list(candles)
        self.atr = atr(self.candles, 14) or (self.candles[-1].close * 0.01 if self.candles else 0.0)
        self.highs, self.lows = label_swings(self.candles, left, right)
        self._mark_swept()
        self._cluster_equals(equal_tol_atr)
        self._score()

    # ------------------------------------------------------------------ build
    def _mark_swept(self) -> None:
        n = len(self.candles)
        for lvl in self.highs:
            after = self.candles[lvl.index + 1:]
            lvl.swept = bool(after) and max(c.high for c in after) > lvl.price
            lvl.displacement = self._displacement(lvl, "high")
        for lvl in self.lows:
            after = self.candles[lvl.index + 1:]
            lvl.swept = bool(after) and min(c.low for c in after) < lvl.price
            lvl.displacement = self._displacement(lvl, "low")

    def _displacement(self, lvl: StructuralLevel, kind: str) -> float:
        window = self.candles[lvl.index + 1: lvl.index + 6]
        if not window or self.atr <= 0:
            return 0.0
        if kind == "low":
            return (max(c.high for c in window) - lvl.price) / self.atr
        return (lvl.price - min(c.low for c in window)) / self.atr

    def _cluster_equals(self, tol_atr: float) -> None:
        tol = self.atr * tol_atr
        for group in (self.highs, self.lows):
            for i, a in enumerate(group):
                for b in group[i + 1:]:
                    if abs(a.price - b.price) <= tol:
                        a.equal_count += 1
                        b.equal_count += 1

    def _score(self) -> None:
        for lvl in self.highs + self.lows:
            score = 0.0
            score += 0.35 if lvl.label in ("HH", "HL", "LH", "LL") else 0.15
            score += min(0.30, 0.15 * (lvl.equal_count - 1))     # equal levels stack
            score += min(0.25, lvl.displacement / 8.0)           # violent reaction
            score += 0.10 if not lvl.swept else 0.0              # still untapped
            lvl.strength = min(1.0, score)

    # ----------------------------------------------------------------- query
    def untapped(self, kind: str) -> List[StructuralLevel]:
        src = self.highs if kind == "high" else self.lows
        return [l for l in src if not l.swept]

    def pools_above(self, price: float, max_pct: float = 0.06) -> List[StructuralLevel]:
        out = [l for l in self.highs if l.price > price
               and (l.price - price) / price <= max_pct]
        out.sort(key=lambda l: l.price)
        return out

    def pools_below(self, price: float, max_pct: float = 0.06) -> List[StructuralLevel]:
        out = [l for l in self.lows if l.price < price
               and (price - l.price) / price <= max_pct]
        out.sort(key=lambda l: -l.price)
        return out

    def nearest_pool(self, direction: str, price: float,
                     min_distance_pct: float = 0.0008) -> Optional[StructuralLevel]:
        """The next structural liquidity pocket in the direction of the trade."""
        pools = self.pools_above(price) if direction == "LONG" else self.pools_below(price)
        for p in pools:
            if abs(p.price - price) / price >= min_distance_pct:
                return p
        return None

    def resting_bias(self, price: float, span_pct: float = 0.05) -> Dict[str, object]:
        """
        Which side holds more resting liquidity right now.

        This is the "where is the money" question. Untapped pools are weighted
        by strength and by proximity, because a huge pool 5% away pulls less
        than a decent one 0.5% away.
        """
        def weigh(levels: List[StructuralLevel], sign: int) -> float:
            total = 0.0
            for l in levels:
                if l.swept:
                    continue
                dist = abs(l.price - price) / price
                if dist > span_pct or dist <= 0:
                    continue
                total += l.strength * (1.0 - dist / span_pct)
            return total

        above = weigh(self.pools_above(price, span_pct), 1)
        below = weigh(self.pools_below(price, span_pct), -1)
        total = above + below
        if total <= 0:
            return {"above": 0.0, "below": 0.0, "bias": "balanced", "ratio": 1.0}
        ratio = (above + 1e-9) / (below + 1e-9)
        bias = "above" if ratio > 1.35 else ("below" if ratio < 0.74 else "balanced")
        return {"above": round(above, 3), "below": round(below, 3),
                "bias": bias, "ratio": round(ratio, 2)}

    def supports_direction(self, direction: str, price: float) -> bool:
        """
        Is the trade heading toward liquidity rather than away from it?

        A long wants the bulk of untapped liquidity sitting ABOVE - that is
        what price is being driven toward once the lows have been swept.
        """
        bias = self.resting_bias(price)
        if direction == "LONG":
            return bias["bias"] in ("above", "balanced")
        return bias["bias"] in ("below", "balanced")

    def summary(self, price: float) -> dict:
        bias = self.resting_bias(price)
        above = self.pools_above(price)[:3]
        below = self.pools_below(price)[:3]
        return {
            "bias": bias,
            "above": [l.to_dict() for l in above],
            "below": [l.to_dict() for l in below],
            "untapped_highs": len(self.untapped("high")),
            "untapped_lows": len(self.untapped("low")),
        }

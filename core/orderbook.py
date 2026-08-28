"""
Order book & heatmap logic.

The book is never a standalone signal. A large resting bid means nothing by
itself - what matters is what happens when price actually reaches it:

    Case A  price approaches -> the bid vanishes            -> spoof / pull -> IGNORE
    Case B  price arrives -> market sells hit the bid ->
            the bid is consumed -> price does not break     -> real absorption -> INTERESTING

So we track individual walls through time and classify their fate, rather
than counting resting size at a single instant.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from .utils import get_logger, mean, safe_float

log = get_logger("orderbook")


@dataclass
class Wall:
    price: float
    side: str                 # "bid" | "ask"
    first_qty: float
    peak_qty: float
    last_qty: float
    first_ts: int
    last_ts: int
    min_distance: float = 1e9   # closest price ever got, as a fraction
    touched: bool = False       # price actually reached the level
    history: Deque[Tuple[int, float]] = field(default_factory=lambda: deque(maxlen=80))

    @property
    def decay(self) -> float:
        """How much of the wall has gone (1.0 = fully removed)."""
        if self.peak_qty <= 0:
            return 0.0
        return max(0.0, 1.0 - (self.last_qty / self.peak_qty))

    def verdict(self) -> str:
        if self.touched and self.decay >= 0.40:
            return "consumed"
        if not self.touched and self.decay >= 0.60:
            return "pulled"
        if self.decay < 0.25:
            return "resting"
        return "eroding"


class DepthTracker:
    """Consumes partial-depth snapshots and keeps a bounded wall registry."""

    MAX_WALLS = 40

    def __init__(self, wall_mult: float = 3.0, keep: int = 240, touch_tolerance: float = 0.0008):
        self.wall_mult = wall_mult
        self.touch_tolerance = touch_tolerance
        self.snapshots: Deque[Tuple[int, float, float, float, float]] = deque(maxlen=keep)
        self.walls: Dict[Tuple[str, float], Wall] = {}
        self.updates = 0
        self.last_bid = 0.0
        self.last_ask = 0.0

    # ------------------------------------------------------------------ ingest
    def update(self, bids: List[List[str]], asks: List[List[str]], ts: int) -> None:
        b = [(safe_float(p), safe_float(q)) for p, q in bids if safe_float(q) > 0]
        a = [(safe_float(p), safe_float(q)) for p, q in asks if safe_float(q) > 0]
        if not b or not a:
            return

        self.updates += 1
        self.last_bid = b[0][0]
        self.last_ask = a[0][0]
        mid = (self.last_bid + self.last_ask) / 2
        bid_sum = sum(q for _, q in b)
        ask_sum = sum(q for _, q in a)
        self.snapshots.append((ts, mid, bid_sum, ask_sum, self.last_ask - self.last_bid))

        avg = mean([q for _, q in b] + [q for _, q in a])
        if avg <= 0:
            return
        threshold = avg * self.wall_mult

        for side, book in (("bid", b), ("ask", a)):
            for price, qty in book:
                key = (side, round(price, 10))
                w = self.walls.get(key)
                if w is None:
                    if qty < threshold:
                        continue
                    w = Wall(price=price, side=side, first_qty=qty, peak_qty=qty,
                             last_qty=qty, first_ts=ts, last_ts=ts)
                    self.walls[key] = w
                else:
                    w.peak_qty = max(w.peak_qty, qty)
                    w.last_qty = qty
                    w.last_ts = ts
                w.history.append((ts, qty))
                dist = abs(mid - price) / max(1e-12, mid)
                w.min_distance = min(w.min_distance, dist)
                if dist <= self.touch_tolerance:
                    w.touched = True

        # levels that disappeared from the visible book decay to zero
        visible = {("bid", round(p, 10)) for p, _ in b} | {("ask", round(p, 10)) for p, _ in a}
        for key, w in self.walls.items():
            if key not in visible and w.last_ts < ts:
                w.last_qty = 0.0
                w.last_ts = ts
                dist = abs(mid - w.price) / max(1e-12, mid)
                w.min_distance = min(w.min_distance, dist)
                if dist <= self.touch_tolerance:
                    w.touched = True

        if len(self.walls) > self.MAX_WALLS:
            for key in sorted(self.walls, key=lambda k: self.walls[k].last_ts)[: len(self.walls) - self.MAX_WALLS]:
                self.walls.pop(key, None)

    # -------------------------------------------------------------- analytics
    def stacking(self) -> Dict[str, float]:
        """Resting size imbalance across the visible book, smoothed."""
        if not self.snapshots:
            return {"bid_ask_ratio": 1.0, "spread_pct": 0.0, "samples": 0}
        recent = list(self.snapshots)[-30:]
        bid = mean([s[2] for s in recent])
        ask = mean([s[3] for s in recent])
        mid = mean([s[1] for s in recent]) or 1.0
        return {
            "bid_ask_ratio": (bid / ask) if ask > 0 else 1.0,
            "spread_pct": mean([s[4] for s in recent]) / mid,
            "samples": len(recent),
        }

    def wall_report(self, direction: str, price: float, max_distance: float = 0.006) -> Dict[str, object]:
        """Fate of the walls sitting on our side of the trade."""
        side = "bid" if direction == "LONG" else "ask"
        relevant = [w for (s, _), w in self.walls.items()
                    if s == side and abs(w.price - price) / max(1e-12, price) <= max_distance]
        verdicts = [w.verdict() for w in relevant]
        consumed = verdicts.count("consumed")
        pulled = verdicts.count("pulled")
        resting = verdicts.count("resting")
        total = max(1, len(relevant))
        best = max(relevant, key=lambda w: w.peak_qty, default=None)
        return {
            "count": len(relevant),
            "consumed": consumed,
            "pulled": pulled,
            "resting": resting,
            "pull_ratio": pulled / total,
            "case_b": consumed >= 1,                       # real execution + hold
            "case_a": pulled >= 1 and consumed == 0,       # spoof behaviour
            "biggest": None if best is None else {
                "price": best.price, "peak": round(best.peak_qty, 3),
                "decay": round(best.decay, 2), "verdict": best.verdict(),
            },
        }

    def analyse(self, direction: str, price: float, cfg) -> Dict[str, object]:
        stack = self.stacking()
        walls = self.wall_report(direction, price)
        ratio = stack["bid_ask_ratio"]
        supportive = ratio >= cfg.ob_stack_ratio if direction == "LONG" else \
            (ratio <= (1.0 / cfg.ob_stack_ratio) if ratio > 0 else False)
        return {
            "stack_ratio": round(ratio, 3),
            "spread_pct": round(stack["spread_pct"], 6),
            "supportive": bool(supportive),
            "walls": walls,
            "liquidity_pulling": bool(walls["pull_ratio"] > cfg.ob_pull_ratio_max and walls["count"] >= 2),
            "updates": self.updates,
        }

    def clear(self) -> None:
        self.snapshots.clear()
        self.walls.clear()

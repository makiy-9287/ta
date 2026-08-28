"""Core data structures used across the engine."""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from .utils import safe_float


# --------------------------------------------------------------------- candles
@dataclass(slots=True)
class Candle:
    ts: int              # open time (ms)
    open: float
    high: float
    low: float
    close: float
    volume: float        # base volume
    quote_volume: float
    trades: int
    taker_buy: float     # taker buy base volume
    close_ts: int = 0

    @classmethod
    def from_rest(cls, row: list) -> "Candle":
        return cls(
            ts=int(row[0]),
            open=safe_float(row[1]),
            high=safe_float(row[2]),
            low=safe_float(row[3]),
            close=safe_float(row[4]),
            volume=safe_float(row[5]),
            close_ts=int(row[6]),
            quote_volume=safe_float(row[7]),
            trades=int(row[8]),
            taker_buy=safe_float(row[9]),
        )

    @classmethod
    def from_ws(cls, k: dict) -> "Candle":
        return cls(
            ts=int(k["t"]),
            open=safe_float(k["o"]),
            high=safe_float(k["h"]),
            low=safe_float(k["l"]),
            close=safe_float(k["c"]),
            volume=safe_float(k["v"]),
            close_ts=int(k["T"]),
            quote_volume=safe_float(k.get("q", 0)),
            trades=int(k.get("n", 0)),
            taker_buy=safe_float(k.get("V", 0)),
        )

    # -- derived -------------------------------------------------------------
    @property
    def delta(self) -> float:
        """Taker buy volume minus taker sell volume (kline-derived delta)."""
        return (2.0 * self.taker_buy) - self.volume

    @property
    def range(self) -> float:
        return max(1e-12, self.high - self.low)

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def body_ratio(self) -> float:
        return self.body / self.range

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def body_low(self) -> float:
        return min(self.open, self.close)

    @property
    def body_high(self) -> float:
        return max(self.open, self.close)

    @property
    def bullish(self) -> bool:
        return self.close >= self.open


# ----------------------------------------------------------------------- zones
@dataclass
class Zone:
    symbol: str
    kind: str                    # "demand" (support) | "supply" (resistance)
    tf: str                      # anchor timeframe, e.g. "4h"
    low: float
    high: float
    created_ts: int = 0
    members: int = 1
    touches: int = 0             # tests after formation
    last_test_ts: int = 0
    score: int = 0
    grade: str = ""              # "A+", "A", ""
    breakdown: Dict[str, int] = field(default_factory=dict)
    flags: Dict[str, Any] = field(default_factory=dict)
    id: Optional[int] = None

    @property
    def mid(self) -> float:
        return (self.low + self.high) / 2.0

    @property
    def height(self) -> float:
        return max(1e-12, self.high - self.low)

    @property
    def direction(self) -> str:
        return "LONG" if self.kind == "demand" else "SHORT"

    def contains(self, price: float, buffer_frac: float = 0.0) -> bool:
        pad = self.height * buffer_frac
        return (self.low - pad) <= price <= (self.high + pad)

    def distance_frac(self, price: float) -> float:
        """Distance from price to the zone edge, as a fraction of zone height."""
        if self.contains(price):
            return 0.0
        if price < self.low:
            return (self.low - price) / self.height
        return (price - self.high) / self.height

    def overlaps(self, other: "Zone") -> bool:
        return min(self.high, other.high) >= max(self.low, other.low)

    def to_row(self) -> dict:
        return {
            "symbol": self.symbol,
            "kind": self.kind,
            "tf": self.tf,
            "low": self.low,
            "high": self.high,
            "created_ts": self.created_ts,
            "members": self.members,
            "touches": self.touches,
            "score": self.score,
            "grade": self.grade,
            "meta": json.dumps({"breakdown": self.breakdown, "flags": self.flags}),
        }


# ------------------------------------------------------------------- decisions
@dataclass
class Decision:
    passed: bool
    direction: str = ""
    confidence: float = 0.0
    reasons: List[str] = field(default_factory=list)     # confirmations found
    blockers: List[str] = field(default_factory=list)    # why it was skipped
    details: Dict[str, Any] = field(default_factory=dict)

    def add(self, ok: bool, tag: str) -> bool:
        if ok:
            self.reasons.append(tag)
        return ok

    def block(self, tag: str) -> None:
        if tag not in self.blockers:
            self.blockers.append(tag)


# --------------------------------------------------------------------- signals
@dataclass
class Signal:
    symbol: str
    direction: str            # LONG | SHORT
    entry_low: float
    entry_high: float
    entry_ref: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    grade: str
    zone_score: int
    confidence: float
    reasons: List[str] = field(default_factory=list)
    zone_low: float = 0.0
    zone_high: float = 0.0
    risk_pct: float = 0.0
    rr: float = 0.0
    decimals: int = 4
    created_ts: int = 0
    meta: Dict[str, Any] = field(default_factory=dict)
    id: Optional[int] = None

    def to_row(self) -> dict:
        d = asdict(self)
        d["reasons"] = json.dumps(self.reasons)
        d["meta"] = json.dumps(self.meta)
        d.pop("id", None)
        return d

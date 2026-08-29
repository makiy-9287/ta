"""
Normalised market events.

Every exchange speaks its own dialect. Binance marks the *maker* side, Bybit
marks the *taker* side; Binance streams full depth snapshots, Bybit streams
deltas; intervals are "5m" on one and "5" on the other. All of that is
resolved in the exchange adapters, and nothing downstream of this module ever
learns which venue a print came from.

These objects are deliberately small and slotted - millions of them pass
through the queue in a session.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .models import Candle

Level = Tuple[float, float]        # (price, size)


@dataclass(slots=True)
class TradeEvent:
    """One aggressive execution. `buy` is the AGGRESSOR side, always."""
    exchange: str
    symbol: str
    price: float
    qty: float
    buy: bool
    ts: int
    tid: int = 0

    @property
    def notional(self) -> float:
        return self.price * self.qty


@dataclass(slots=True)
class DepthEvent:
    """A full top-N book snapshot, already reconstructed from deltas."""
    exchange: str
    symbol: str
    bids: List[Level]
    asks: List[Level]
    ts: int

    @property
    def mid(self) -> float:
        if not self.bids or not self.asks:
            return 0.0
        return (self.bids[0][0] + self.asks[0][0]) / 2.0


@dataclass(slots=True)
class KlineEvent:
    exchange: str
    symbol: str
    interval: str          # always normalised to Binance notation: 1m/3m/5m/15m/1h/4h
    candle: Candle
    closed: bool


@dataclass(slots=True)
class TickerEvent:
    exchange: str
    symbol: str
    price: float
    ts: int


MarketEvent = object      # TradeEvent | DepthEvent | KlineEvent | TickerEvent

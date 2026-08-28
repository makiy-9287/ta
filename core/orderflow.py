"""
Order-flow core: footprint, delta, absorption, imbalance.

Everything here is built from aggTrade data. Binance marks each aggregated
trade with `m` (isBuyerMaker):

    m = True   -> the buyer was the maker, so the AGGRESSOR WAS A SELLER
    m = False  -> the seller was the maker, so the AGGRESSOR WAS A BUYER

That single flag is what separates passive liquidity from aggression, and
aggression vs. resulting price movement is the whole game:

    huge aggressive selling + price refuses to fall  ->  passive buyers are
    absorbing  ->  bullish clue at support (and the mirror at resistance).

Memory: buckets older than the window are dropped on every insert, so a
symbol that stays armed for hours costs the same as one armed for minutes.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from .utils import clamp, get_logger, mean, percentile, stdev

log = get_logger("orderflow")


@dataclass
class Bucket:
    ts: int
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    buy_vol: float = 0.0     # aggressive buys  (ask side lifted)
    sell_vol: float = 0.0    # aggressive sells (bid side hit)
    trades: int = 0
    levels: Dict[float, List[float]] = field(default_factory=dict)  # price -> [sell, buy]

    @property
    def delta(self) -> float:
        return self.buy_vol - self.sell_vol

    @property
    def volume(self) -> float:
        return self.buy_vol + self.sell_vol


class FootprintBook:
    """Rolling footprint: time buckets x price levels x (bid/ask volume)."""

    def __init__(self, tick_size: float, ref_price: float, bucket_sec: int = 60,
                 window_min: int = 45, price_bins: int = 60):
        self.bucket_ms = max(1, bucket_sec) * 1000
        self.window_ms = max(1, window_min) * 60 * 1000
        self.tick_size = max(tick_size, 1e-12)
        self.grid = self._make_grid(ref_price, tick_size, price_bins)
        self.buckets: Dict[int, Bucket] = {}
        self.recent: Deque[Tuple[int, float, float, bool]] = deque(maxlen=4000)
        self.total_trades = 0
        self.first_ts = 0
        self.last_ts = 0

    @staticmethod
    def _make_grid(ref_price: float, tick: float, bins: int) -> float:
        """Price-level granularity: fine enough to see imbalances, coarse
        enough that a level actually accumulates volume."""
        target = max(ref_price * 0.00035, tick)
        steps = max(1, round(target / max(tick, 1e-12)))
        return steps * max(tick, 1e-12)

    def _snap(self, price: float) -> float:
        return round(int(price / self.grid) * self.grid, 10)

    # ------------------------------------------------------------------ ingest
    def add(self, price: float, qty: float, is_buyer_maker: bool, ts: int) -> None:
        if qty <= 0 or price <= 0:
            return
        key = (ts // self.bucket_ms) * self.bucket_ms
        b = self.buckets.get(key)
        if b is None:
            b = Bucket(ts=key, open=price, high=price, low=price, close=price)
            self.buckets[key] = b
        b.high = max(b.high, price)
        b.low = min(b.low, price)
        b.close = price
        b.trades += 1

        lvl = self._snap(price)
        slot = b.levels.get(lvl)
        if slot is None:
            slot = [0.0, 0.0]
            b.levels[lvl] = slot
        if is_buyer_maker:
            b.sell_vol += qty
            slot[0] += qty
        else:
            b.buy_vol += qty
            slot[1] += qty

        self.recent.append((ts, price, qty, is_buyer_maker))
        self.total_trades += 1
        self.first_ts = self.first_ts or ts
        self.last_ts = ts
        self._prune(ts)

    def seed_from_rest(self, trades: List[dict]) -> None:
        for t in trades:
            try:
                self.add(float(t["p"]), float(t["q"]), bool(t["m"]), int(t["T"]))
            except (KeyError, TypeError, ValueError):
                continue

    def _prune(self, now: int) -> None:
        cutoff = now - self.window_ms
        if len(self.buckets) > 4 and min(self.buckets) < cutoff:
            for k in [k for k in self.buckets if k < cutoff]:
                self.buckets.pop(k, None)

    # ------------------------------------------------------------------ views
    def ordered(self, window_sec: Optional[int] = None) -> List[Bucket]:
        keys = sorted(self.buckets)
        if window_sec and keys:
            cutoff = self.last_ts - window_sec * 1000
            keys = [k for k in keys if k >= cutoff]
        return [self.buckets[k] for k in keys]

    def deltas(self, window_sec: Optional[int] = None) -> List[float]:
        return [b.delta for b in self.ordered(window_sec)]

    def cvd(self, window_sec: Optional[int] = None) -> List[float]:
        run = 0.0
        out = []
        for b in self.ordered(window_sec):
            run += b.delta
            out.append(run)
        return out

    def totals(self, window_sec: Optional[int] = None) -> Dict[str, float]:
        bs = self.ordered(window_sec)
        buy = sum(b.buy_vol for b in bs)
        sell = sum(b.sell_vol for b in bs)
        return {"buy": buy, "sell": sell, "delta": buy - sell, "volume": buy + sell,
                "trades": sum(b.trades for b in bs), "buckets": len(bs)}

    def price_path(self, window_sec: Optional[int] = None) -> Dict[str, float]:
        bs = self.ordered(window_sec)
        if not bs:
            return {}
        return {
            "open": bs[0].open,
            "high": max(b.high for b in bs),
            "low": min(b.low for b in bs),
            "close": bs[-1].close,
        }

    def merged_levels(self, window_sec: Optional[int] = None) -> Dict[float, List[float]]:
        out: Dict[float, List[float]] = {}
        for b in self.ordered(window_sec):
            for lvl, (s, bu) in b.levels.items():
                slot = out.setdefault(lvl, [0.0, 0.0])
                slot[0] += s
                slot[1] += bu
        return out

    # -------------------------------------------------------------- analytics
    def delta_extreme(self, direction: str, z_threshold: float = 1.3,
                      window_sec: Optional[int] = None) -> Dict[str, object]:
        """Is there a statistically extreme delta print in the window?"""
        ds = self.deltas(window_sec)
        res = {"found": False, "z": 0.0, "value": 0.0}
        if len(ds) < 6:
            return res
        sd = stdev(ds)
        mu = mean(ds)
        if sd <= 0:
            return res
        if direction == "LONG":
            val = min(ds)
            z = (val - mu) / sd
            res.update(found=z <= -z_threshold, z=round(z, 2), value=round(val, 4))
        else:
            val = max(ds)
            z = (val - mu) / sd
            res.update(found=z >= z_threshold, z=round(z, 2), value=round(val, 4))
        return res

    def absorption(self, direction: str, vol_mult: float = 1.9,
                   recovery_frac: float = 0.45, window_sec: int = 900) -> Dict[str, object]:
        """
        Aggression into a level that fails to move price = passive absorption.

        LONG  : heavy aggressive SELL volume parked at the lows of the window,
                yet price recovers back up through the window range.
        SHORT : mirror image with aggressive BUY volume at the highs.
        """
        res = {"found": False, "ratio": 0.0, "share": 0.0, "recovery": 0.0, "level": 0.0}
        levels = self.merged_levels(window_sec)
        path = self.price_path(window_sec)
        if len(levels) < 4 or not path:
            return res

        rng = path["high"] - path["low"]
        if rng <= 0:
            return res

        edge = path["low"] + rng * 0.28 if direction == "LONG" else path["high"] - rng * 0.28
        if direction == "LONG":
            edge_levels = {p: v for p, v in levels.items() if p <= edge}
        else:
            edge_levels = {p: v for p, v in levels.items() if p >= edge}
        if not edge_levels:
            return res

        level_totals = [s + b for s, b in levels.values()]
        avg_level = mean(level_totals)
        if avg_level <= 0:
            return res

        aggressive = sum(v[0] for v in edge_levels.values()) if direction == "LONG" \
            else sum(v[1] for v in edge_levels.values())
        edge_total = sum(sum(v) for v in edge_levels.values())
        share = aggressive / edge_total if edge_total > 0 else 0.0
        ratio = aggressive / avg_level

        recovery = (path["close"] - path["low"]) / rng if direction == "LONG" \
            else (path["high"] - path["close"]) / rng

        hot = max(edge_levels.items(), key=lambda kv: (kv[1][0] if direction == "LONG" else kv[1][1]))
        res.update(
            found=bool(ratio >= vol_mult and share >= 0.55 and recovery >= recovery_frac),
            ratio=round(ratio, 2), share=round(share, 3),
            recovery=round(recovery, 3), level=hot[0],
        )
        return res

    def imbalances(self, ratio: float = 3.0, window_sec: int = 900) -> Dict[str, object]:
        """
        Diagonal footprint imbalance: ask volume at P+1 vs bid volume at P.
        Stacked imbalances (3+ in a row) are the meaningful ones.
        """
        levels = self.merged_levels(window_sec)
        prices = sorted(levels)
        buy_imb: List[float] = []
        sell_imb: List[float] = []
        for lo, hi in zip(prices, prices[1:]):
            bid_lo = levels[lo][0]
            ask_hi = levels[hi][1]
            if ask_hi > 0 and bid_lo * ratio <= ask_hi:
                buy_imb.append(hi)
            if bid_lo > 0 and ask_hi * ratio <= bid_lo:
                sell_imb.append(lo)

        def longest_stack(seq: List[float]) -> int:
            if not seq:
                return 0
            best = run = 1
            for a, b in zip(seq, seq[1:]):
                run = run + 1 if abs(b - a) <= self.grid * 1.5 else 1
                best = max(best, run)
            return best

        return {
            "buy_count": len(buy_imb), "sell_count": len(sell_imb),
            "buy_stack": longest_stack(buy_imb), "sell_stack": longest_stack(sell_imb),
            "buy_levels": buy_imb[-6:], "sell_levels": sell_imb[-6:],
            "clean": bool(buy_imb or sell_imb),
        }

    def aggression_at(self, price: float, tolerance_levels: int = 2) -> Dict[str, float]:
        """Volume split right around a specific price (used for sweep bars)."""
        tol = self.grid * tolerance_levels
        sell = buy = 0.0
        for lvl, (s, b) in self.merged_levels().items():
            if abs(lvl - price) <= tol:
                sell += s
                buy += b
        return {"sell": sell, "buy": buy, "delta": buy - sell}

    def health(self, min_trades: int) -> Dict[str, object]:
        t = self.totals()
        span = (self.last_ts - self.first_ts) / 1000 if self.last_ts and self.first_ts else 0
        return {
            "trades": t["trades"], "buckets": t["buckets"], "span_sec": int(span),
            "enough": t["trades"] >= min_trades and t["buckets"] >= 5,
            "memory_levels": sum(len(b.levels) for b in self.buckets.values()),
        }

    def clear(self) -> None:
        self.buckets.clear()
        self.recent.clear()


def footprint_table(book: FootprintBook, direction: str, rows: int = 6,
                    window_sec: int = 900, decimals: int = 2) -> List[Tuple[float, float, float, float]]:
    """Compact (price, bid, ask, delta) rows around the action - for Telegram."""
    levels = book.merged_levels(window_sec)
    if not levels:
        return []
    ordered = sorted(levels.items(), key=lambda kv: kv[0], reverse=(direction == "SHORT"))
    picked = ordered[:rows] if direction == "SHORT" else ordered[:rows]
    out = []
    for price, (sell, buy) in sorted(picked, key=lambda kv: kv[0], reverse=True):
        out.append((round(price, decimals), round(sell, 2), round(buy, 2), round(buy - sell, 2)))
    return out

"""
Market data plumbing: connections in, normalised events out, through queues.

The rule that shapes this whole module: **a websocket callback never computes
anything**. It parses the frame, normalises it, and drops it in a queue. All
delta/CVD/footprint/heatmap maths happens in a separate worker task.

Why it matters: aggTrade and order book frames arrive in bursts of hundreds
per second across a dozen symbols. If the socket coroutine does the analytics
inline, the TCP receive buffer backs up behind it, frames queue in the kernel,
and the feed silently falls minutes behind while still looking connected. By
the time the strategy sees a print it is stale, and the "live" order flow it
is reading is fiction.

Queues are sharded by symbol so every symbol's events stay strictly ordered
within one worker, while the workers themselves run independently.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Optional

from .events import DepthEvent, KlineEvent, TickerEvent, TradeEvent
from .utils import get_logger

log = get_logger("feed")

Consumer = Callable[[object], Awaitable[None]]


class FeedStats:
    def __init__(self) -> None:
        self.pushed = 0
        self.processed = 0
        self.dropped = 0
        self.by_exchange: Dict[str, int] = defaultdict(int)
        self.max_lag_ms = 0.0
        self.lag_ms = 0.0
        self.max_depth = 0
        self._lag_samples = 0

    def note_lag(self, lag_ms: float) -> None:
        if lag_ms < 0 or lag_ms > 600_000:
            return                       # clock skew between venues, ignore
        self._lag_samples += 1
        # exponential moving average keeps this O(1) and drift-free
        self.lag_ms += (lag_ms - self.lag_ms) / min(self._lag_samples, 200)
        self.max_lag_ms = max(self.max_lag_ms, lag_ms)

    def snapshot(self, depths: List[int]) -> dict:
        return {
            "pushed": self.pushed, "processed": self.processed, "dropped": self.dropped,
            "pending": sum(depths), "max_depth": self.max_depth,
            "lag_ms": round(self.lag_ms, 1), "max_lag_ms": round(self.max_lag_ms, 1),
            "by_exchange": dict(self.by_exchange),
        }


class EventBus:
    """Sharded queues plus the worker tasks that drain them."""

    def __init__(self, shards: int = 2, maxsize: int = 20000, batch: int = 256):
        self.shards = max(1, shards)
        self.batch = max(1, batch)
        self.queues: List[asyncio.Queue] = [asyncio.Queue(maxsize=maxsize)
                                            for _ in range(self.shards)]
        self.stats = FeedStats()
        self._consumer: Optional[Consumer] = None
        self._tasks: List[asyncio.Task] = []
        self._running = False

    def shard_for(self, symbol: str) -> int:
        # deterministic per symbol: ordering is preserved where it matters
        return (hash(symbol) & 0x7FFFFFFF) % self.shards

    # ------------------------------------------------------------------ push
    def publish(self, event) -> None:
        """
        Called from the socket coroutine. Never awaits, never computes.

        If a shard is full the OLDEST event is discarded rather than the
        newest: under pressure, current market state matters more than a
        complete history, and blocking here would stall the socket.
        """
        q = self.queues[self.shard_for(getattr(event, "symbol", ""))]
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            try:
                q.get_nowait()
                q.task_done()
                self.stats.dropped += 1
            except asyncio.QueueEmpty:
                pass
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                self.stats.dropped += 1
                return
        self.stats.pushed += 1
        self.stats.by_exchange[getattr(event, "exchange", "?")] += 1
        depth = q.qsize()
        if depth > self.stats.max_depth:
            self.stats.max_depth = depth

    # --------------------------------------------------------------- consume
    def start(self, consumer: Consumer) -> None:
        self._consumer = consumer
        self._running = True
        self._tasks = [asyncio.create_task(self._worker(i), name=f"flow-worker-{i}")
                       for i in range(self.shards)]

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def _worker(self, shard: int) -> None:
        q = self.queues[shard]
        while self._running:
            try:
                first = await q.get()
                batch = [first]
                # drain what is already waiting: one context switch per burst
                # instead of per event
                while len(batch) < self.batch:
                    try:
                        batch.append(q.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                now = time.time() * 1000
                ts = getattr(batch[-1], "ts", 0) or getattr(
                    getattr(batch[-1], "candle", None), "ts", 0)
                if ts:
                    self.stats.note_lag(now - ts)

                for ev in batch:
                    try:
                        await self._consumer(ev)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        log.debug("consumer error on %s: %s", type(ev).__name__, exc)
                    finally:
                        self.stats.processed += 1
                        q.task_done()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("worker %d error: %s", shard, exc)
                await asyncio.sleep(0.5)

    @property
    def depths(self) -> List[int]:
        return [q.qsize() for q in self.queues]

    def health(self) -> dict:
        return self.stats.snapshot(self.depths)


class SymbolRouter:
    """
    Decides which venue streams which symbol.

    Splitting the watchlist across two exchanges halves the websocket load and
    the connection count on each IP, and means one venue rate-limiting or
    blocking us degrades half the coverage instead of all of it.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.assignment: Dict[str, str] = {}
        self.available: Dict[str, set] = {}

    def plan(self, symbols: List[str], listings: Dict[str, set],
             enabled: List[str]) -> Dict[str, str]:
        """
        `listings` maps exchange -> set of symbols it lists.
        Symbols on a single venue go there; the rest alternate, so the split
        stays balanced by rank (and therefore roughly by volume).
        """
        self.available = listings
        assignment: Dict[str, str] = {}
        counts = {ex: 0 for ex in enabled}
        if not enabled:
            return assignment

        for symbol in symbols:
            venues = [ex for ex in enabled if symbol in listings.get(ex, set())]
            if not venues:
                continue
            if len(venues) == 1:
                choice = venues[0]
            else:
                # keep the two sides even; ties break toward the primary venue
                choice = min(venues, key=lambda ex: (counts[ex], enabled.index(ex)))
            assignment[symbol] = choice
            counts[choice] += 1

        self.assignment = assignment
        log.info("stream assignment: %s",
                 ", ".join(f"{ex}={n}" for ex, n in counts.items()))
        return assignment

    def venue(self, symbol: str, default: str = "binance") -> str:
        return self.assignment.get(symbol, default)

    def summary(self) -> Dict[str, int]:
        out: Dict[str, int] = defaultdict(int)
        for ex in self.assignment.values():
            out[ex] += 1
        return dict(out)


class PriceBook:
    """Merged last/mark prices from every source, newest wins."""

    def __init__(self) -> None:
        self._prices: Dict[str, float] = {}
        self._ts: Dict[str, float] = {}
        self.sources: Dict[str, str] = {}

    def update(self, symbol: str, price: float, source: str) -> None:
        if price > 0:
            self._prices[symbol] = price
            self._ts[symbol] = time.time()
            self.sources[symbol] = source

    def bulk_update(self, prices: Dict[str, float], source: str) -> None:
        now = time.time()
        for sym, px in prices.items():
            if px > 0:
                self._prices[sym] = px
                self._ts[sym] = now
                self.sources[sym] = source

    def get(self, symbol: str, default: float = 0.0) -> float:
        return self._prices.get(symbol, default)

    def age(self, symbol: str) -> float:
        ts = self._ts.get(symbol)
        return (time.time() - ts) if ts else float("inf")

    def as_dict(self) -> Dict[str, float]:
        return self._prices

    def prune(self, keep: set) -> None:
        for sym in list(self._prices):
            if sym not in keep:
                self._prices.pop(sym, None)
                self._ts.pop(sym, None)
                self.sources.pop(sym, None)

    def __len__(self) -> int:
        return len(self._prices)

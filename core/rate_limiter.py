"""
Sliding-window request-weight limiter.

Binance USDT-M futures allows 2400 weight / minute / IP. We run on a far
smaller self-imposed budget and additionally honour the X-MBX-USED-WEIGHT-1M
header the exchange returns, so a shared/NAT'd VPS IP can never push us into
a 418 ban.

The header value is a *snapshot* of the exchange's rolling minute at the time
of the response, so it has to decay with age: a reading of 1097 taken forty
seconds ago represents far less live pressure than the same number taken now.
Treating it as permanent deadlocks the entire process - one full zone rebuild
would leave the budget exhausted forever, with no further request able to
refresh the reading that is blocking it.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Deque, Tuple

from .utils import get_logger

log = get_logger("ratelimit")

WINDOW = 60.0


class WeightLimiter:
    def __init__(self, budget_per_min: int = 1100, bulk_share: float = 0.65):
        self.budget = max(60, int(budget_per_min))
        # background work (the periodic zone rebuild) may only ever consume
        # part of the budget, so latency-sensitive calls - prices, arming,
        # trade monitoring - always have headroom left to them
        self.bulk_budget = max(30, int(self.budget * bulk_share))
        self._events: Deque[Tuple[float, int]] = deque()
        self._lock = asyncio.Lock()
        self._reported_weight = 0
        self._reported_at = 0.0
        self._banned_until = 0.0
        self._waits = 0

    def _prune(self, now: float) -> None:
        cutoff = now - WINDOW
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def _local_since(self, since: float) -> int:
        return sum(w for ts, w in self._events if ts >= since)

    def _effective(self, now: float) -> int:
        """
        Current pressure on the IP.

        The exchange's own header is ground truth, so when we have a recent
        reading we anchor to it and add only what we have spent since it was
        taken. Our local sliding window is a fallback estimate for when no
        reading is available.

        This matters: the exchange counts in fixed one-minute buckets that
        reset on the boundary, while a sliding window keeps charging us for
        requests the exchange has already forgotten. Trusting the window alone
        made the engine throttle itself against pressure that no longer
        existed - a full minute of self-inflicted stalling after every zone
        rebuild.
        """
        local = self._local_since(now - WINDOW)
        if not self._reported_at:
            return local
        age = now - self._reported_at
        if age >= WINDOW:
            return local
        return self._reported_weight + self._local_since(self._reported_at)

    def _effective_reported(self, now: float) -> int:
        if not self._reported_at or (now - self._reported_at) >= WINDOW:
            return 0
        return self._reported_weight

    @property
    def used(self) -> int:
        now = time.time()
        self._prune(now)
        return self._effective(now)

    @property
    def reported(self) -> int:
        return self._effective_reported(time.time())

    async def acquire(self, weight: int, bulk: bool = False) -> None:
        """
        Reserve request weight. `bulk=True` marks background work that must
        yield to latency-sensitive calls.
        """
        weight = max(1, int(weight))
        cap = self.bulk_budget if bulk else self.budget
        waited = 0.0
        while True:
            async with self._lock:
                now = time.time()
                if now < self._banned_until:
                    wait = self._banned_until - now
                else:
                    self._prune(now)
                    effective = self._effective(now)
                    if effective + weight <= cap:
                        self._events.append((now, weight))
                        return
                    # wait for the oldest local event to age out, or for the
                    # decaying header reading to leave room
                    if self._events:
                        wait = max(0.05, WINDOW - (now - self._events[0][0]))
                    else:
                        wait = max(0.05, WINDOW - (now - self._reported_at))
            step = min(wait, 5.0)
            waited += step
            self._waits += 1
            if waited >= 20.0 and self._waits % 8 == 0:
                log.warning("waiting %.0fs on the weight budget "
                            "(effective=%d cap=%d reported=%d age=%.0fs)",
                            waited, self._effective(time.time()), cap, self._reported_weight,
                            time.time() - self._reported_at if self._reported_at else -1)
            await asyncio.sleep(step)

    def sync_from_header(self, used_weight: int) -> None:
        """Record the exchange's own accounting, timestamped so it can decay."""
        if used_weight > 0:
            self._reported_weight = used_weight
            self._reported_at = time.time()
            if used_weight > self.budget * 1.4:
                log.warning("exchange reports weight %s (budget %s) - backing off",
                            used_weight, self.budget)

    def penalise(self, seconds: float) -> None:
        self._banned_until = max(self._banned_until, time.time() + seconds)
        log.warning("rate-limit penalty: pausing REST for %.0fs", seconds)

    @property
    def blocked_for(self) -> float:
        return max(0.0, self._banned_until - time.time())

    def snapshot(self) -> dict:
        return {
            "used_local": self.used,
            "bulk_budget": self.bulk_budget,
            "used_reported": self.reported,
            "raw_reported": self._reported_weight,
            "budget": self.budget,
            "waits": self._waits,
            "penalty_sec": round(self.blocked_for),
        }

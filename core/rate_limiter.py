"""
Sliding-window request-weight limiter.

Binance USDT-M futures allows 2400 weight / minute / IP. We run on a far
smaller self-imposed budget and additionally honour the X-MBX-USED-WEIGHT-1M
header the exchange returns, so a shared/NAT'd VPS IP can never push us into
a 418 ban.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Deque, Tuple

from .utils import get_logger

log = get_logger("ratelimit")


class WeightLimiter:
    def __init__(self, budget_per_min: int = 1100):
        self.budget = max(60, int(budget_per_min))
        self._events: Deque[Tuple[float, int]] = deque()
        self._lock = asyncio.Lock()
        self._reported_weight = 0
        self._banned_until = 0.0

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    @property
    def used(self) -> int:
        self._prune(time.time())
        return sum(w for _, w in self._events)

    @property
    def reported(self) -> int:
        return self._reported_weight

    async def acquire(self, weight: int) -> None:
        weight = max(1, int(weight))
        while True:
            async with self._lock:
                now = time.time()
                if now < self._banned_until:
                    wait = self._banned_until - now
                else:
                    self._prune(now)
                    local = sum(w for _, w in self._events)
                    effective = max(local, self._reported_weight)
                    if effective + weight <= self.budget:
                        self._events.append((now, weight))
                        return
                    wait = max(0.05, 60.0 - (now - self._events[0][0])) if self._events else 1.0
            log.debug("weight budget reached, sleeping %.2fs", wait)
            await asyncio.sleep(min(wait, 5.0))

    def sync_from_header(self, used_weight: int) -> None:
        """Trust the exchange's own accounting when it is higher than ours."""
        if used_weight > 0:
            self._reported_weight = used_weight
            if used_weight > self.budget * 1.4:
                log.warning("exchange reports weight %s (budget %s) - backing off", used_weight, self.budget)

    def penalise(self, seconds: float) -> None:
        self._banned_until = max(self._banned_until, time.time() + seconds)
        log.warning("rate-limit penalty: pausing REST for %.0fs", seconds)

    def snapshot(self) -> dict:
        return {"used_local": self.used, "used_reported": self._reported_weight, "budget": self.budget}

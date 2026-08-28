"""
WebSocket layer.

Two kinds of connection:

1. MarkPriceStream  - a single connection to !markPrice@arr@1s that gives us
   live prices for *every* symbol. Zero REST weight for proximity checks and
   for monitoring open setups.

2. SymbolStream     - opened only for a symbol that has been *armed* (price is
   inside a high-grade zone) and closed the moment it disarms. Carries
   aggTrade + partial depth + 1m/3m/5m klines. This is what keeps memory flat:
   we never hold order-flow state for symbols we are not actively hunting.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Awaitable, Callable, Dict, List, Optional

import websockets

from .utils import get_logger, safe_float

log = get_logger("ws")

Handler = Callable[[str, dict], Awaitable[None]]


class _BaseStream:
    def __init__(self, url: str, name: str):
        self.url = url
        self.name = name
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.last_msg_ts = 0.0
        self.reconnects = 0

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name=f"ws-{self.name}")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None

    @property
    def alive(self) -> bool:
        return bool(self._task and not self._task.done())

    @property
    def stale_for(self) -> float:
        return time.time() - self.last_msg_ts if self.last_msg_ts else 1e9

    async def _handle(self, payload: dict) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    async def _run(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20,
                    close_timeout=5, max_queue=512,
                ) as sock:
                    log.info("ws connected: %s", self.name)
                    backoff = 1.0
                    async for raw in sock:
                        if not self._running:
                            break
                        self.last_msg_ts = time.time()
                        try:
                            payload = json.loads(raw)
                        except (ValueError, TypeError):
                            continue
                        await self._handle(payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                if not self._running:
                    return
                self.reconnects += 1
                log.warning("ws %s dropped (%s) - reconnect in %.0fs", self.name, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class MarkPriceStream(_BaseStream):
    """One connection, every symbol's mark price, updated once per second."""

    def __init__(self, ws_base: str):
        super().__init__(f"{ws_base}/ws/!markPrice@arr@1s", "markprice")
        self.prices: Dict[str, float] = {}
        self.updated_ts: float = 0.0

    async def _handle(self, payload) -> None:
        items = payload if isinstance(payload, list) else [payload]
        for it in items:
            sym = it.get("s")
            if sym:
                self.prices[sym] = safe_float(it.get("p"))
        self.updated_ts = time.time()

    def get(self, symbol: str, default: float = 0.0) -> float:
        return self.prices.get(symbol, default)

    def prune(self, keep: set) -> None:
        """Drop prices for symbols no longer tracked (memory hygiene)."""
        for sym in list(self.prices.keys()):
            if sym not in keep:
                self.prices.pop(sym, None)


class SymbolStream(_BaseStream):
    """Per-symbol combined stream feeding the order-flow engine."""

    def __init__(self, ws_base: str, symbol: str, handler: Handler,
                 depth_levels: int = 20, depth_speed_ms: int = 500,
                 intervals: Optional[List[str]] = None):
        self.symbol = symbol.upper()
        low = self.symbol.lower()
        intervals = intervals or ["1m", "3m", "5m"]
        parts = [f"{low}@aggTrade", f"{low}@depth{depth_levels}@{depth_speed_ms}ms"]
        parts += [f"{low}@kline_{iv}" for iv in intervals]
        url = f"{ws_base}/stream?streams=" + "/".join(parts)
        super().__init__(url, f"flow:{self.symbol}")
        self.handler = handler
        self.messages = 0

    async def _handle(self, payload: dict) -> None:
        data = payload.get("data") if "data" in payload else payload
        if not isinstance(data, dict):
            return
        event = data.get("e", "")
        self.messages += 1
        try:
            await self.handler(event, data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.debug("handler error for %s/%s: %s", self.symbol, event, exc)

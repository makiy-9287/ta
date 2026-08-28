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
    def __init__(self, url: str, name: str, idle_timeout: float = 60.0):
        self.urls = [url] if isinstance(url, str) else list(url)
        self.url = self.urls[0]
        self.name = name
        self.idle_timeout = idle_timeout
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._url_index = 0
        self.last_msg_ts = 0.0
        self.messages = 0
        self.reconnects = 0
        self.silent_drops = 0

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
        """Seconds since the last payload. Never-received reports as inf."""
        return (time.time() - self.last_msg_ts) if self.last_msg_ts else float("inf")

    @property
    def healthy(self) -> bool:
        return self.alive and self.stale_for < max(self.idle_timeout, 30.0)

    def describe_staleness(self) -> str:
        return "no data since connect" if not self.last_msg_ts else f"{self.stale_for:.0f}s"

    async def _handle(self, payload: dict) -> None:  # pragma: no cover - overridden
        raise NotImplementedError

    async def _run(self) -> None:
        backoff = 1.0
        while self._running:
            self.url = self.urls[self._url_index % len(self.urls)]
            got_data = False
            try:
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=20,
                    close_timeout=5, max_queue=512,
                ) as sock:
                    log.info("ws connected: %s", self.name)
                    while self._running:
                        # A socket that opens, answers pings and never sends a
                        # payload is indistinguishable from a healthy one at the
                        # transport layer - some networks and middleboxes park
                        # connections exactly like that. Treat silence as a
                        # failure instead of waiting forever inside `async for`.
                        try:
                            raw = await asyncio.wait_for(sock.recv(), timeout=self.idle_timeout)
                        except asyncio.TimeoutError:
                            raise ConnectionError(
                                f"no data for {self.idle_timeout:.0f}s") from None
                        got_data = True
                        self.last_msg_ts = time.time()
                        self.messages += 1
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
                if not got_data:
                    # this endpoint gave us nothing at all - try the next
                    # variant next time round (if the stream defines any)
                    self.silent_drops += 1
                    self._url_index += 1
                    if len(self.urls) > 1:
                        log.warning("ws %s silent on %s - switching endpoint",
                                    self.name, self.url.rsplit("/", 1)[-1][:48])
                log.warning("ws %s dropped (%s) - reconnect in %.0fs", self.name, exc, backoff)
                await asyncio.sleep(backoff)
                # once an endpoint has proven repeatedly silent, stop hammering
                # it - the network is likely blocking us, and connection
                # attempts are themselves rate limited by the exchange
                ceiling = 120.0 if self.silent_drops >= 5 else 30.0
                backoff = min(backoff * 2, ceiling)


class MarkPriceStream(_BaseStream):
    """One connection, every symbol's mark price, updated once per second."""

    def __init__(self, ws_base: str, idle_timeout: float = 45.0):
        # two endpoint spellings: the 1s variant and the default 3s one, plus
        # the combined-stream form. If the network silently swallows one we
        # rotate to the next rather than sitting on a dead socket.
        super().__init__(
            [
                f"{ws_base}/ws/!markPrice@arr@1s",
                f"{ws_base}/stream?streams=!markPrice@arr@1s",
                f"{ws_base}/ws/!markPrice@arr",
            ],
            "markprice", idle_timeout=idle_timeout,
        )
        self.prices: Dict[str, float] = {}
        self.updated_ts: float = 0.0

    async def _handle(self, payload) -> None:
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]          # combined-stream wrapper
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
                 intervals: Optional[List[str]] = None, idle_timeout: float = 90.0):
        self.symbol = symbol.upper()
        low = self.symbol.lower()
        intervals = intervals or ["1m", "3m", "5m"]
        parts = [f"{low}@aggTrade", f"{low}@depth{depth_levels}@{depth_speed_ms}ms"]
        parts += [f"{low}@kline_{iv}" for iv in intervals]
        url = f"{ws_base}/stream?streams=" + "/".join(parts)
        super().__init__(url, f"flow:{self.symbol}", idle_timeout=idle_timeout)
        self.handler = handler

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

"""
WebSocket layer.

Two kinds of connection:

1. MarkPriceStream  - a single connection to !markPrice@arr@1s that gives us
   live prices for *every* symbol. Zero REST weight for proximity checks and
   for monitoring open setups.

2. SessionStream    - opened only for a symbol that has been *armed* (price is
   inside a high-grade zone) and closed the moment it disarms. It is driven by
   an exchange StreamSession, so the same class carries a Binance combined
   stream or a Bybit subscription without knowing the difference. This is what
   keeps memory flat: we never hold order-flow state for symbols we are not
   actively hunting.
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
    QUIET_AFTER = 6          # stop shouting once the network is clearly the problem

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

    async def _on_connect(self, sock) -> None:
        """Hook for venues that need subscribe frames after the handshake."""
        return None

    def _start_keepalive(self, sock):
        """Hook for venues needing application-level pings."""
        return None

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
                    await self._on_connect(sock)
                    keeper = self._start_keepalive(sock)
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
                    if self.silent_drops == self.QUIET_AFTER:
                        log.warning(
                            "ws %s: %d endpoints tried, none delivered data. This host "
                            "appears unable to receive Binance websocket traffic; the "
                            "engine will keep retrying quietly and run on REST polling.",
                            self.name, self.silent_drops)
                    elif self.silent_drops < self.QUIET_AFTER and len(self.urls) > 1:
                        log.warning("ws %s silent on %s - switching endpoint",
                                    self.name, self.url.rsplit("/", 1)[-1][:48])
                speak = log.warning if self.silent_drops < self.QUIET_AFTER else log.debug
                speak("ws %s dropped (%s) - reconnect in %.0fs", self.name, exc, backoff)
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


class SessionStream(_BaseStream):
    """
    A stream driven by an exchange `StreamSession`.

    The session owns the venue dialect: which frames to send on connect, how
    often to ping at the application layer, and how to turn raw payloads into
    normalised events. This class owns only the socket lifecycle.

    The handler is called with a LIST of events and must not block - it exists
    to drop them into the queue and return.
    """

    def __init__(self, session, on_events, name: str, idle_timeout: float = 120.0):
        super().__init__(session.url, name, idle_timeout=idle_timeout)
        self.session = session
        self.on_events = on_events
        self._keeper: Optional[asyncio.Task] = None

    async def _on_connect(self, sock) -> None:
        for msg in self.session.subscribe_messages():
            await sock.send(json.dumps(msg))
            await asyncio.sleep(0.05)

    def _start_keepalive(self, sock):
        payload = self.session.keepalive_message()
        interval = self.session.keepalive_interval()
        if not payload or interval <= 0:
            return None

        async def _ping() -> None:
            # Bybit expects an application-level ping; a protocol ping is not
            # enough to keep the subscription alive
            while self._running:
                await asyncio.sleep(interval)
                try:
                    await sock.send(json.dumps(payload))
                except Exception:  # noqa: BLE001
                    return

        self._keeper = asyncio.create_task(_ping(), name=f"ping-{self.name}")
        return self._keeper

    async def _handle(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        if payload.get("op") in ("pong", "ping") or payload.get("ret_msg") == "pong":
            return
        if payload.get("success") is not None and "op" in payload:
            return                                    # subscribe acknowledgement
        events = self.session.handle(payload)
        if events:
            self.on_events(events)

    async def stop(self) -> None:
        if self._keeper:
            self._keeper.cancel()
            self._keeper = None
        await super().stop()

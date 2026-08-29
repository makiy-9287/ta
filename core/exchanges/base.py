"""Adapter contract plus a shared HTTP client."""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import aiohttp

from ..events import DepthEvent, TradeEvent
from ..models import Candle
from ..utils import get_logger, safe_float

log = get_logger("exchange")


class PermanentRequestError(RuntimeError):
    """A 4xx: retrying cannot help and would only waste request weight."""


class HttpClient:
    """Small retrying JSON client shared by the adapters."""

    def __init__(self, base: str, limiter, timeout: int = 20, name: str = ""):
        self.base = base.rstrip("/")
        self.limiter = limiter
        self.name = name
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"User-Agent": "sniper-flow/2.0"},
                connector=aiohttp.TCPConnector(limit=32, ttl_dns_cache=300),
            )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None,
                  weight: int = 1, retries: int = 3, bulk: bool = False) -> Any:
        await self.start()
        url = f"{self.base}{path}"
        last: Optional[Exception] = None
        for attempt in range(retries):
            if self.limiter:
                await self.limiter.acquire(weight, bulk=bulk)
            try:
                async with self._session.get(url, params=params) as resp:
                    used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    if used and self.limiter:
                        self.limiter.sync_from_header(int(safe_float(used)))
                    if resp.status in (418, 429):
                        retry_after = float(resp.headers.get("Retry-After", 30))
                        if self.limiter:
                            self.limiter.penalise(max(retry_after, 30))
                        raise RuntimeError(f"rate limited ({resp.status})")
                    if resp.status >= 500:
                        raise RuntimeError(f"{self.name} {resp.status}")
                    if resp.status != 200:
                        body = await resp.text()
                        if 400 <= resp.status < 500:
                            # a client error will not fix itself: an unlisted
                            # symbol stays unlisted. Retrying burns request
                            # weight three times over for nothing.
                            raise PermanentRequestError(
                                f"http {resp.status}: {body[:180]}")
                        raise RuntimeError(f"http {resp.status}: {body[:180]}")
                    return await resp.json()
            except asyncio.CancelledError:
                raise
            except PermanentRequestError:
                raise
            except Exception as exc:  # noqa: BLE001
                last = exc
                await asyncio.sleep(min(20.0, 1.5 * (2 ** attempt)))
        raise last if last else RuntimeError("request failed")


class StreamSession:
    """
    Per-connection websocket state.

    Bybit sends order book *deltas*, so a session has to maintain a local book
    and emit reconstructed snapshots. Binance sends full snapshots and needs no
    state, but both live behind the same interface.
    """

    url: str = ""

    def subscribe_messages(self) -> List[dict]:
        return []

    def keepalive_message(self) -> Optional[dict]:
        return None

    def keepalive_interval(self) -> float:
        return 0.0

    def handle(self, raw: dict) -> List[object]:
        raise NotImplementedError


class ExchangeAdapter:
    name = ""
    has_taker_volume = False      # can klines give us historical delta?
    intervals: Dict[str, str] = {}

    async def start(self) -> None: ...
    async def close(self) -> None: ...

    async def instruments(self) -> Dict[str, dict]:
        """symbol -> {tick_size, decimals, base}"""
        raise NotImplementedError

    async def tickers(self) -> List[dict]:
        """[{symbol, quote_volume, price}]"""
        raise NotImplementedError

    async def klines(self, symbol: str, interval: str, limit: int,
                     bulk: bool = False) -> List[Candle]:
        raise NotImplementedError

    async def recent_trades(self, symbol: str, limit: int = 1000) -> List[TradeEvent]:
        raise NotImplementedError

    async def depth(self, symbol: str, limit: int = 50) -> Optional[DepthEvent]:
        raise NotImplementedError

    def flow_session(self, symbol: str, intervals: List[str], depth_levels: int,
                     depth_speed_ms: int) -> StreamSession:
        raise NotImplementedError

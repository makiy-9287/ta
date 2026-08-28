"""
Binance USDT-M Futures public REST client.

Public endpoints only - no API key, no signing, no order placement anywhere
in this project. Every call is weight-accounted before it leaves the process.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import aiohttp

from .models import Candle
from .rate_limiter import WeightLimiter
from .utils import get_logger, safe_float

log = get_logger("rest")

# endpoint -> weight (per Binance fapi docs)
KLINE_WEIGHTS = ((100, 1), (500, 2), (1000, 5))


def kline_weight(limit: int) -> int:
    for cap, w in KLINE_WEIGHTS:
        if limit <= cap:
            return w
    return 10


def depth_weight(limit: int) -> int:
    if limit <= 50:
        return 2
    if limit <= 100:
        return 5
    if limit <= 500:
        return 10
    return 20


class BinanceREST:
    def __init__(self, base: str, limiter: WeightLimiter, timeout: int = 20):
        self.base = base.rstrip("/")
        self.limiter = limiter
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers={"User-Agent": "sniper-flow/1.0"},
                connector=aiohttp.TCPConnector(limit=32, ttl_dns_cache=300),
            )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None,
                   weight: int = 1, retries: int = 3) -> Any:
        await self.start()
        url = f"{self.base}{path}"
        last_err: Optional[Exception] = None

        for attempt in range(retries):
            await self.limiter.acquire(weight)
            try:
                async with self._session.get(url, params=params) as resp:
                    used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    if used:
                        self.limiter.sync_from_header(int(safe_float(used)))

                    if resp.status in (418, 429):
                        retry_after = float(resp.headers.get("Retry-After", 30))
                        self.limiter.penalise(max(retry_after, 30 if resp.status == 429 else 120))
                        raise RuntimeError(f"rate limited ({resp.status})")

                    if resp.status >= 500:
                        raise RuntimeError(f"binance {resp.status}")

                    if resp.status != 200:
                        body = await resp.text()
                        raise RuntimeError(f"http {resp.status}: {body[:180]}")

                    return await resp.json()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                backoff = min(20.0, 1.5 * (2 ** attempt))
                log.debug("GET %s failed (%s) - retry in %.1fs", path, exc, backoff)
                await asyncio.sleep(backoff)

        log.warning("GET %s failed permanently: %s", path, last_err)
        raise last_err if last_err else RuntimeError("request failed")

    # ------------------------------------------------------------------ market
    async def exchange_info(self) -> dict:
        return await self._get("/fapi/v1/exchangeInfo", weight=1)

    async def ticker_24h(self) -> List[dict]:
        return await self._get("/fapi/v1/ticker/24hr", weight=40)

    async def mark_prices(self) -> List[dict]:
        return await self._get("/fapi/v1/premiumIndex", weight=10)

    async def ticker_prices(self) -> List[dict]:
        """Every symbol's last price for weight 2 - the cheap fallback when
        the mark price stream is unavailable."""
        return await self._get("/fapi/v1/ticker/price", weight=2)

    async def klines(self, symbol: str, interval: str, limit: int = 500,
                     end_time: Optional[int] = None) -> List[Candle]:
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
        if end_time:
            params["endTime"] = end_time
        raw = await self._get("/fapi/v1/klines", params, weight=kline_weight(limit))
        return [Candle.from_rest(r) for r in raw]

    async def depth(self, symbol: str, limit: int = 100) -> dict:
        return await self._get("/fapi/v1/depth", {"symbol": symbol, "limit": limit},
                               weight=depth_weight(limit))

    async def agg_trades(self, symbol: str, limit: int = 1000) -> List[dict]:
        return await self._get("/fapi/v1/aggTrades", {"symbol": symbol, "limit": limit}, weight=20)

    async def ping(self) -> bool:
        try:
            await self._get("/fapi/v1/ping", weight=1, retries=1)
            return True
        except Exception:
            return False

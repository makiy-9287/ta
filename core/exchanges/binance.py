"""Binance USDT-M futures adapter."""
from __future__ import annotations

from typing import Dict, List, Optional

from ..events import DepthEvent, KlineEvent, TradeEvent
from ..models import Candle
from ..utils import decimals_from_tick, get_logger, safe_float
from .base import ExchangeAdapter, HttpClient, StreamSession

log = get_logger("binance")

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


class BinanceStreamSession(StreamSession):
    """Combined stream: trades + partial depth (full snapshots) + klines."""

    def __init__(self, ws_base: str, symbol: str, intervals: List[str],
                 depth_levels: int, depth_speed_ms: int):
        self.symbol = symbol.upper()
        low = self.symbol.lower()
        parts = [f"{low}@aggTrade", f"{low}@depth{depth_levels}@{depth_speed_ms}ms"]
        parts += [f"{low}@kline_{iv}" for iv in intervals]
        self.url = f"{ws_base}/stream?streams=" + "/".join(parts)

    def handle(self, raw: dict) -> List[object]:
        data = raw.get("data") if "data" in raw else raw
        if not isinstance(data, dict):
            return []
        event = data.get("e", "")

        if event == "aggTrade":
            return [TradeEvent(
                exchange="binance", symbol=self.symbol,
                price=safe_float(data.get("p")), qty=safe_float(data.get("q")),
                # Binance flags the MAKER side: buyer-is-maker means the
                # aggressor was a seller
                buy=not bool(data.get("m")),
                ts=int(data.get("T") or data.get("E") or 0),
                tid=int(safe_float(data.get("a"), 0)),
            )]

        if event == "depthUpdate" or ("b" in data and "a" in data and "e" not in data):
            return [DepthEvent(
                exchange="binance", symbol=self.symbol,
                bids=[(safe_float(p), safe_float(q)) for p, q in data.get("b", [])],
                asks=[(safe_float(p), safe_float(q)) for p, q in data.get("a", [])],
                ts=int(data.get("T") or data.get("E") or 0),
            )]

        if event == "kline":
            k = data.get("k") or {}
            return [KlineEvent(
                exchange="binance", symbol=self.symbol, interval=str(k.get("i")),
                candle=Candle.from_ws(k), closed=bool(k.get("x")),
            )]
        return []


class BinanceAdapter(ExchangeAdapter):
    name = "binance"
    has_taker_volume = True
    intervals = {iv: iv for iv in ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")}

    def __init__(self, cfg, limiter):
        self.cfg = cfg
        self.http = HttpClient(cfg.rest_base, limiter, cfg.rest_timeout, "binance")
        self.ws_base = cfg.ws_base

    async def start(self) -> None:
        await self.http.start()

    async def close(self) -> None:
        await self.http.close()

    async def instruments(self) -> Dict[str, dict]:
        info = await self.http.get("/fapi/v1/exchangeInfo", weight=1)
        out: Dict[str, dict] = {}
        for s in info.get("symbols", []):
            if s.get("status") != "TRADING" or s.get("contractType") != "PERPETUAL":
                continue
            if s.get("quoteAsset") != self.cfg.quote_asset:
                continue
            tick = 0.0
            for f in s.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick = safe_float(f.get("tickSize"))
            out[s["symbol"]] = {"tick_size": tick or 0.01,
                                "decimals": decimals_from_tick(tick or 0.01),
                                "base": s.get("baseAsset", "")}
        return out

    async def tickers(self) -> List[dict]:
        rows = await self.http.get("/fapi/v1/ticker/24hr", weight=40, bulk=True)
        return [{"symbol": r.get("symbol", ""),
                 "quote_volume": safe_float(r.get("quoteVolume")),
                 "price": safe_float(r.get("lastPrice"))} for r in rows]

    async def prices(self) -> Dict[str, float]:
        rows = await self.http.get("/fapi/v1/ticker/price", weight=2)
        return {r["symbol"]: safe_float(r.get("price")) for r in rows if r.get("symbol")}

    async def klines(self, symbol: str, interval: str, limit: int,
                     bulk: bool = False) -> List[Candle]:
        raw = await self.http.get("/fapi/v1/klines",
                                  {"symbol": symbol, "interval": interval, "limit": limit},
                                  weight=kline_weight(limit), bulk=bulk)
        return [Candle.from_rest(r) for r in raw]

    async def recent_trades(self, symbol: str, limit: int = 1000) -> List[TradeEvent]:
        raw = await self.http.get("/fapi/v1/aggTrades", {"symbol": symbol, "limit": limit},
                                  weight=20)
        return [TradeEvent(exchange="binance", symbol=symbol,
                           price=safe_float(t.get("p")), qty=safe_float(t.get("q")),
                           buy=not bool(t.get("m")), ts=int(t.get("T", 0)),
                           tid=int(safe_float(t.get("a"), 0))) for t in raw]

    async def depth(self, symbol: str, limit: int = 50) -> Optional[DepthEvent]:
        d = await self.http.get("/fapi/v1/depth", {"symbol": symbol, "limit": limit},
                                weight=depth_weight(limit))
        return DepthEvent(exchange="binance", symbol=symbol,
                          bids=[(safe_float(p), safe_float(q)) for p, q in d.get("bids", [])],
                          asks=[(safe_float(p), safe_float(q)) for p, q in d.get("asks", [])],
                          ts=int(d.get("E", 0)))

    def flow_session(self, symbol: str, intervals: List[str], depth_levels: int,
                     depth_speed_ms: int) -> StreamSession:
        return BinanceStreamSession(self.ws_base, symbol, intervals, depth_levels, depth_speed_ms)

    def mark_price_url(self) -> str:
        return f"{self.ws_base}/ws/!markPrice@arr@1s"

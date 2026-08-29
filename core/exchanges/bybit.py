"""
Bybit v5 linear-perpetual adapter.

Three things differ from Binance and are resolved here so nothing downstream
has to care:

  * Bybit tags the TAKER side on a trade ("S": "Buy" means an aggressive buy),
    while Binance tags the maker side.
  * Bybit streams an order book SNAPSHOT followed by DELTAS, so this session
    maintains a local book and emits reconstructed top-N snapshots. It also
    throttles them - the raw linear feed pushes every 20ms, which would flood
    the queue for no analytical gain.
  * Bybit klines carry no taker-buy volume, so historical delta cannot be
    derived from them (`has_taker_volume = False`). Candle history for scoring
    is therefore sourced from Binance; Bybit supplies the live tape.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from ..events import DepthEvent, KlineEvent, TradeEvent
from ..models import Candle
from ..utils import decimals_from_tick, get_logger, safe_float
from .base import ExchangeAdapter, HttpClient, StreamSession

log = get_logger("bybit")

# our canonical (Binance) notation -> Bybit's
INTERVALS = {"1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
             "1h": "60", "2h": "120", "4h": "240", "1d": "D"}
REVERSE = {v: k for k, v in INTERVALS.items()}


def _candle_from_rest(row: list) -> Candle:
    # [start, open, high, low, close, volume, turnover]
    ts = int(safe_float(row[0]))
    vol = safe_float(row[5])
    return Candle(ts=ts, open=safe_float(row[1]), high=safe_float(row[2]),
                  low=safe_float(row[3]), close=safe_float(row[4]),
                  volume=vol, quote_volume=safe_float(row[6]), trades=0,
                  taker_buy=0.0, close_ts=ts)


class BybitStreamSession(StreamSession):
    def __init__(self, ws_base: str, symbol: str, intervals: List[str],
                 depth_levels: int, depth_speed_ms: int):
        self.symbol = symbol.upper()
        self.url = f"{ws_base}/v5/public/linear"
        self.depth_levels = depth_levels
        self.depth_speed_ms = max(100, depth_speed_ms)
        self._book_depth = 50 if depth_levels <= 50 else 200
        self._bids: Dict[float, float] = {}
        self._asks: Dict[float, float] = {}
        self._last_depth_emit = 0
        self._intervals = intervals
        self._topics = [f"publicTrade.{self.symbol}",
                        f"orderbook.{self._book_depth}.{self.symbol}"]
        self._topics += [f"kline.{INTERVALS.get(iv, '1')}.{self.symbol}" for iv in intervals]

    def subscribe_messages(self) -> List[dict]:
        # Bybit caps a subscribe frame at 10 topics
        return [{"op": "subscribe", "args": self._topics[i:i + 10]}
                for i in range(0, len(self._topics), 10)]

    def keepalive_message(self) -> Optional[dict]:
        return {"op": "ping"}

    def keepalive_interval(self) -> float:
        return 20.0

    # ------------------------------------------------------------------ book
    def _apply(self, side: Dict[float, float], levels: List[list]) -> None:
        for price, size in levels:
            p, q = safe_float(price), safe_float(size)
            if q <= 0:
                side.pop(p, None)
            else:
                side[p] = q

    def _snapshot(self, ts: int) -> DepthEvent:
        bids = sorted(self._bids.items(), key=lambda kv: -kv[0])[: self.depth_levels]
        asks = sorted(self._asks.items(), key=lambda kv: kv[0])[: self.depth_levels]
        return DepthEvent(exchange="bybit", symbol=self.symbol, bids=bids, asks=asks, ts=ts)

    # --------------------------------------------------------------- dispatch
    def handle(self, raw: dict) -> List[object]:
        topic = raw.get("topic", "")
        if not topic:
            return []
        data = raw.get("data")
        ts = int(safe_float(raw.get("ts"), 0))

        if topic.startswith("publicTrade"):
            out = []
            for t in (data or []):
                out.append(TradeEvent(
                    exchange="bybit", symbol=self.symbol,
                    price=safe_float(t.get("p")), qty=safe_float(t.get("v")),
                    # Bybit reports the TAKER side directly
                    buy=str(t.get("S", "")).lower() == "buy",
                    ts=int(safe_float(t.get("T"), ts)),
                    tid=int(safe_float(t.get("i"), 0)) if str(t.get("i", "")).isdigit() else 0,
                ))
            return out

        if topic.startswith("orderbook"):
            if not isinstance(data, dict):
                return []
            if raw.get("type") == "snapshot":
                self._bids.clear()
                self._asks.clear()
            self._apply(self._bids, data.get("b", []))
            self._apply(self._asks, data.get("a", []))
            if not self._bids or not self._asks:
                return []
            # throttle: the raw linear book pushes every 20ms
            if ts - self._last_depth_emit < self.depth_speed_ms:
                return []
            self._last_depth_emit = ts
            return [self._snapshot(ts)]

        if topic.startswith("kline"):
            out = []
            for k in (data or []):
                iv = REVERSE.get(str(k.get("interval")), "1m")
                vol = safe_float(k.get("volume"))
                candle = Candle(
                    ts=int(safe_float(k.get("start"))), open=safe_float(k.get("open")),
                    high=safe_float(k.get("high")), low=safe_float(k.get("low")),
                    close=safe_float(k.get("close")), volume=vol,
                    quote_volume=safe_float(k.get("turnover")), trades=0,
                    taker_buy=0.0, close_ts=int(safe_float(k.get("end"))),
                )
                out.append(KlineEvent(exchange="bybit", symbol=self.symbol,
                                      interval=iv, candle=candle,
                                      closed=bool(k.get("confirm"))))
            return out
        return []


class BybitAdapter(ExchangeAdapter):
    name = "bybit"
    has_taker_volume = False
    intervals = INTERVALS

    def __init__(self, cfg, limiter):
        self.cfg = cfg
        self.http = HttpClient(cfg.bybit_rest, limiter, cfg.rest_timeout, "bybit")
        self.ws_base = cfg.bybit_ws

    async def start(self) -> None:
        await self.http.start()

    async def close(self) -> None:
        await self.http.close()

    @staticmethod
    def _rows(payload: dict) -> List:
        return ((payload or {}).get("result") or {}).get("list") or []

    async def instruments(self) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        cursor = ""
        for _ in range(8):                       # paginated, 1000 per page
            params = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            payload = await self.http.get("/v5/market/instruments-info", params, weight=1)
            for s in self._rows(payload):
                if s.get("status") != "Trading" or s.get("quoteCoin") != self.cfg.quote_asset:
                    continue
                if s.get("contractType") != "LinearPerpetual":
                    continue
                tick = safe_float((s.get("priceFilter") or {}).get("tickSize"))
                out[s["symbol"]] = {"tick_size": tick or 0.01,
                                    "decimals": decimals_from_tick(tick or 0.01),
                                    "base": s.get("baseCoin", "")}
            cursor = ((payload or {}).get("result") or {}).get("nextPageCursor") or ""
            if not cursor:
                break
        return out

    async def tickers(self) -> List[dict]:
        payload = await self.http.get("/v5/market/tickers", {"category": "linear"},
                                      weight=1, bulk=True)
        return [{"symbol": r.get("symbol", ""),
                 "quote_volume": safe_float(r.get("turnover24h")),
                 "price": safe_float(r.get("lastPrice"))} for r in self._rows(payload)]

    async def prices(self) -> Dict[str, float]:
        payload = await self.http.get("/v5/market/tickers", {"category": "linear"}, weight=1)
        return {r["symbol"]: safe_float(r.get("lastPrice"))
                for r in self._rows(payload) if r.get("symbol")}

    async def klines(self, symbol: str, interval: str, limit: int,
                     bulk: bool = False) -> List[Candle]:
        payload = await self.http.get("/v5/market/kline", {
            "category": "linear", "symbol": symbol,
            "interval": INTERVALS.get(interval, "5"), "limit": min(limit, 1000),
        }, weight=1, bulk=bulk)
        rows = self._rows(payload)
        # Bybit returns newest first
        return [_candle_from_rest(r) for r in reversed(rows)]

    async def recent_trades(self, symbol: str, limit: int = 1000) -> List[TradeEvent]:
        payload = await self.http.get("/v5/market/recent-trade", {
            "category": "linear", "symbol": symbol, "limit": min(limit, 1000)}, weight=1)
        out = []
        for t in reversed(self._rows(payload)):
            out.append(TradeEvent(exchange="bybit", symbol=symbol,
                                  price=safe_float(t.get("price")),
                                  qty=safe_float(t.get("size")),
                                  buy=str(t.get("side", "")).lower() == "buy",
                                  ts=int(safe_float(t.get("time"), 0))))
        return out

    async def depth(self, symbol: str, limit: int = 50) -> Optional[DepthEvent]:
        payload = await self.http.get("/v5/market/orderbook", {
            "category": "linear", "symbol": symbol, "limit": min(limit, 200)}, weight=1)
        res = (payload or {}).get("result") or {}
        return DepthEvent(exchange="bybit", symbol=symbol,
                          bids=[(safe_float(p), safe_float(q)) for p, q in res.get("b", [])],
                          asks=[(safe_float(p), safe_float(q)) for p, q in res.get("a", [])],
                          ts=int(safe_float(res.get("ts"), 0)))

    def flow_session(self, symbol: str, intervals: List[str], depth_levels: int,
                     depth_speed_ms: int) -> StreamSession:
        return BybitStreamSession(self.ws_base, symbol, intervals, depth_levels, depth_speed_ms)

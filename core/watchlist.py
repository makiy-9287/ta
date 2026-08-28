"""
Watchlist construction.

Every 5 hours (configurable) we rebuild the universe: USDT-margined
perpetuals that are actually trading, with 24h quote volume above the
threshold. Anything thinner than that has an order book too sparse for
footprint work, so it is not worth the bandwidth.
"""
from __future__ import annotations

import time
from typing import Dict, List

from .utils import decimals_from_tick, fmt_usd, get_logger, safe_float

log = get_logger("watchlist")


class WatchlistManager:
    def __init__(self, rest, db, cfg):
        self.rest = rest
        self.db = db
        self.cfg = cfg
        self.symbols: List[str] = []
        self.meta: Dict[str, dict] = {}
        self.last_refresh: float = 0.0
        self.last_volume: Dict[str, float] = {}

    async def load_exchange_info(self) -> None:
        info = await self.rest.exchange_info()
        meta: Dict[str, dict] = {}
        for s in info.get("symbols", []):
            if s.get("status") != "TRADING":
                continue
            if s.get("contractType") != "PERPETUAL":
                continue
            if s.get("quoteAsset") != self.cfg.quote_asset:
                continue
            tick = 0.0
            step = 0.0
            for f in s.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick = safe_float(f.get("tickSize"))
                elif f.get("filterType") == "LOT_SIZE":
                    step = safe_float(f.get("stepSize"))
            meta[s["symbol"]] = {
                "tick_size": tick or 0.01,
                "step_size": step,
                "decimals": decimals_from_tick(tick or 0.01),
                "base": s.get("baseAsset", ""),
            }
        self.meta = meta
        log.info("exchange info: %d tradable %s perpetuals", len(meta), self.cfg.quote_asset)

    async def refresh(self) -> List[str]:
        if not self.meta:
            await self.load_exchange_info()

        tickers = await self.rest.ticker_24h()
        blacklist = self.cfg.blacklist_set
        rows = []
        for t in tickers:
            sym = t.get("symbol", "")
            if sym not in self.meta or sym in blacklist:
                continue
            qv = safe_float(t.get("quoteVolume"))
            if qv < self.cfg.min_quote_volume:
                continue
            rows.append({"symbol": sym, "quote_volume": qv, "price": safe_float(t.get("lastPrice"))})

        rows.sort(key=lambda r: r["quote_volume"], reverse=True)
        rows = rows[: self.cfg.max_watchlist]

        self.symbols = [r["symbol"] for r in rows]
        self.last_volume = {r["symbol"]: r["quote_volume"] for r in rows}
        self.last_refresh = time.time()
        await self.db.save_watchlist(rows)

        if rows:
            log.info("watchlist rebuilt: %d symbols (>= %s 24h volume, top: %s %s)",
                     len(rows), fmt_usd(self.cfg.min_quote_volume),
                     rows[0]["symbol"], fmt_usd(rows[0]["quote_volume"]))
        else:
            log.warning("watchlist empty - is the volume threshold too high?")
        return self.symbols

    def tick_size(self, symbol: str) -> float:
        return self.meta.get(symbol, {}).get("tick_size", 0.01)

    def decimals(self, symbol: str) -> int:
        return self.meta.get(symbol, {}).get("decimals", 4)

    @property
    def due(self) -> bool:
        return (time.time() - self.last_refresh) >= self.cfg.watchlist_refresh_hours * 3600

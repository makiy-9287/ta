"""
Watchlist construction across venues.

Every 5 hours the universe is rebuilt: USDT perpetuals that are actually
trading, with 24h quote volume above the threshold. Instrument lists are
gathered from every configured exchange so the router knows which venue can
stream which symbol - a coin listed on only one of them still gets covered,
it just has no choice of feed.
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Set

from .utils import fmt_usd, get_logger

log = get_logger("watchlist")


class WatchlistManager:
    def __init__(self, adapters: Dict[str, object], db, cfg):
        self.adapters = adapters
        self.db = db
        self.cfg = cfg
        self.symbols: List[str] = []
        self.meta: Dict[str, dict] = {}
        self.listings: Dict[str, Set[str]] = {}
        self.last_refresh: float = 0.0
        self.last_volume: Dict[str, float] = {}

    async def load_instruments(self) -> None:
        listings: Dict[str, Set[str]] = {}
        meta: Dict[str, dict] = {}
        for name, adapter in self.adapters.items():
            try:
                instruments = await adapter.instruments()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s instrument list failed: %s", name, exc)
                listings[name] = set()
                continue
            listings[name] = set(instruments)
            for symbol, info in instruments.items():
                # the primary venue defines tick size and precision so a symbol
                # is formatted identically no matter which feed carries it
                if symbol not in meta or name == self.cfg.history_exchange:
                    meta[symbol] = info
            log.info("%s: %d tradable %s perpetuals", name, len(instruments),
                     self.cfg.quote_asset)
        self.listings = listings
        self.meta = meta

    async def refresh(self) -> List[str]:
        if not self.meta:
            await self.load_instruments()

        volumes: Dict[str, float] = {}
        prices: Dict[str, float] = {}
        for name, adapter in self.adapters.items():
            try:
                rows = await adapter.tickers()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s tickers failed: %s", name, exc)
                continue
            for r in rows:
                sym = r["symbol"]
                if sym not in self.meta:
                    continue
                # take the deeper venue's volume: it is the better read on how
                # much real liquidity the symbol has
                volumes[sym] = max(volumes.get(sym, 0.0), r["quote_volume"])
                if r.get("price"):
                    prices.setdefault(sym, r["price"])

        blacklist = self.cfg.blacklist_set
        rows = [{"symbol": s, "quote_volume": v, "price": prices.get(s, 0.0)}
                for s, v in volumes.items()
                if v >= self.cfg.min_quote_volume and s not in blacklist]
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

    def listed_on(self, symbol: str) -> List[str]:
        return [ex for ex, syms in self.listings.items() if symbol in syms]

    @property
    def due(self) -> bool:
        return (time.time() - self.last_refresh) >= self.cfg.watchlist_refresh_hours * 3600

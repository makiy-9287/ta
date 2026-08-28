"""
The orchestrator.

Task layout (all asyncio, one process):

    watchlist_loop   every 5h   rebuild the volume-filtered universe
    zone_loop        every 4h   600x4H + 600x1H candles -> scored S/R zones
    proximity_loop   every 5m   is CMP inside an A/A+ zone? -> arm the symbol
    arm_loop         every 15s  run the order-flow confirmation on armed symbols
    monitor_loop     every 2s   track open setups against SL / TP1-3
    telegram_loop    always     commands
    housekeeping     every 15m  prune, gc, log memory

Price data for proximity and monitoring comes from one shared
!markPrice@arr stream, so those two loops cost no REST weight at all.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import Counter
from typing import Dict, List, Optional

from config import Settings
from notifier import formatter as fmt
from notifier.telegram import TelegramBot

from .armed import ArmedContext
from .confirm import evaluate
from .database import Database
from .indicators import htf_trend
from .models import Signal, Zone
from .monitor import TradeMonitor
from .rate_limiter import WeightLimiter
from .rest import BinanceREST
from .risk import build_signal
from .utils import (collect_garbage, get_logger, human_delta, now_ms, rss_mb)
from .watchlist import WatchlistManager
from .ws import MarkPriceStream, SymbolStream
from .zones import ZoneEngine

log = get_logger("engine")


class SniperEngine:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.limiter = WeightLimiter(cfg.weight_budget)
        self.rest = BinanceREST(cfg.rest_base, self.limiter, cfg.rest_timeout)
        self.db = Database(cfg.db_path)
        self.bot = TelegramBot(cfg.telegram_token, cfg.telegram_chat_id, cfg.telegram_poll_timeout)
        self.watchlist = WatchlistManager(self.rest, self.db, cfg)
        self.zone_engine = ZoneEngine(cfg)
        self.mark = MarkPriceStream(cfg.ws_base)
        self.monitor = TradeMonitor(self.db, cfg, self._trade_event, self._release_symbol)

        self.zones: Dict[str, List[Zone]] = {}
        self.trend: Dict[str, str] = {}
        self.armed: Dict[str, ArmedContext] = {}
        self.streams: Dict[str, SymbolStream] = {}
        self.cooldown: Dict[str, float] = {}
        self.rest_prices: Dict[str, float] = {}

        self._arm_lock = asyncio.Lock()
        self._tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()

        self.paused = False
        self.started_at = time.time()
        self.cycles = 0
        self.evaluations = 0
        self.rejected = 0
        self.signals_total = 0
        self.signals_today = 0
        self._today = time.strftime("%Y-%m-%d")
        self.blockers: Counter = Counter()
        self.zone_build_ts = 0.0
        self.bot_name = ""

    # ================================================================ lifecycle
    async def start(self) -> None:
        await self.db.init()
        await self.rest.start()
        await self.bot.start()

        ok, name = await self.bot.verify()
        if not ok:
            raise RuntimeError(name)
        self.bot_name = name
        log.info("telegram connected as %s", name)

        self.signals_total = int(await self.db.get_meta("signals_total", 0) or 0)
        await self.monitor.load()
        await self.mark.start()

        log.info("building initial watchlist...")
        await self.watchlist.refresh()
        await self.mark.start()
        await self._rebuild_zones(initial=True)

        if self.cfg.startup_notice:
            await self.bot.send(fmt.startup_message(self._engine_stats(), self.bot_name))

        self._tasks = [
            asyncio.create_task(self._loop_watchlist(), name="watchlist"),
            asyncio.create_task(self._loop_zones(), name="zones"),
            asyncio.create_task(self._loop_proximity(), name="proximity"),
            asyncio.create_task(self._loop_arm(), name="arm"),
            asyncio.create_task(self._loop_monitor(), name="monitor"),
            asyncio.create_task(self._loop_housekeeping(), name="housekeeping"),
            asyncio.create_task(self.bot.poll(self.handle_command), name="telegram"),
        ]
        log.info("engine running with %d tasks", len(self._tasks))

    async def run_forever(self) -> None:
        await self.start()
        await self._stop.wait()

    async def shutdown(self) -> None:
        log.info("shutting down...")
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for sym in list(self.streams):
            await self._disarm(sym, "shutdown")
        await self.mark.stop()
        await self.db.set_meta("signals_total", self.signals_total)
        await self.rest.close()
        await self.bot.close()
        await self.db.close()
        log.info("shutdown complete")

    # ================================================================== loops
    async def _loop_watchlist(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.cfg.watchlist_refresh_hours * 3600)
                await self.watchlist.refresh()
                self.mark.prune(set(self.watchlist.symbols) | set(self.monitor.symbols()))
                for sym in list(self.zones):
                    if sym not in self.watchlist.symbols:
                        self.zones.pop(sym, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("watchlist loop error: %s", exc)
                await asyncio.sleep(60)

    async def _loop_zones(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.cfg.zone_refresh_hours * 3600)
                await self._rebuild_zones()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("zone loop error: %s", exc)
                await asyncio.sleep(120)

    async def _loop_proximity(self) -> None:
        await asyncio.sleep(5)
        while not self._stop.is_set():
            try:
                self.cycles += 1
                await self._scan_proximity()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("proximity loop error: %s", exc)
            await asyncio.sleep(self.cfg.proximity_interval_sec)

    async def _loop_arm(self) -> None:
        while not self._stop.is_set():
            try:
                await self._evaluate_armed()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("arm loop error: %s", exc)
            await asyncio.sleep(self.cfg.eval_interval_sec)

    async def _loop_monitor(self) -> None:
        while not self._stop.is_set():
            try:
                prices = await self._prices()
                if prices:
                    await self.monitor.on_prices(prices)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("monitor loop error: %s", exc)
            await asyncio.sleep(self.cfg.monitor_tick_sec)

    async def _loop_housekeeping(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.cfg.housekeeping_minutes * 60)
            try:
                now = time.time()
                for sym, until in list(self.cooldown.items()):
                    if until < now:
                        self.cooldown.pop(sym, None)
                self.mark.prune(set(self.watchlist.symbols) | set(self.monitor.symbols()))
                await self.db.housekeeping()
                freed = collect_garbage()
                today = time.strftime("%Y-%m-%d")
                if today != self._today:
                    self._today = today
                    self.signals_today = 0
                log.info("housekeeping: %.0f MB rss, %d objects freed, armed=%d, weight=%s",
                         rss_mb(), freed, len(self.armed), self.limiter.snapshot())
                if not self.mark.alive or self.mark.stale_for > 120:
                    log.warning("mark price stream stale (%.0fs) - restarting", self.mark.stale_for)
                    await self.mark.stop()
                    await self.mark.start()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("housekeeping error: %s", exc)

    # ============================================================ zone building
    async def _rebuild_zones(self, initial: bool = False) -> None:
        symbols = list(self.watchlist.symbols)
        if not symbols:
            return
        log.info("rebuilding zones for %d symbols...", len(symbols))
        started = time.time()
        built = 0

        for batch in _batches(symbols, 5):
            if self._stop.is_set():
                return
            results = await asyncio.gather(*[self._zones_for(s) for s in batch],
                                           return_exceptions=True)
            for sym, res in zip(batch, results):
                if isinstance(res, Exception):
                    log.debug("zone build failed for %s: %s", sym, res)
                    continue
                built += 1
            await asyncio.sleep(0.05)

        self.zone_build_ts = time.time()
        total = sum(len(z) for z in self.zones.values())
        log.info("zones ready: %d symbols, %d graded zones in %.0fs",
                 built, total, time.time() - started)
        if initial and total == 0:
            log.warning("no zones passed the %d point threshold on the first pass", self.cfg.score_a)

    async def _zones_for(self, symbol: str) -> None:
        cfg = self.cfg
        if symbol in self.armed or self.monitor.has(symbol):
            return  # never move the goalposts mid-setup
        htf = await self.rest.klines(symbol, cfg.htf_interval, cfg.candle_limit)
        mtf = await self.rest.klines(symbol, cfg.mtf_interval, cfg.candle_limit)
        if len(htf) < 100:
            return
        zones = self.zone_engine.build(symbol, htf, mtf)
        self.trend[symbol] = str(htf_trend(htf)["state"])
        if zones:
            self.zones[symbol] = zones
            await self.db.replace_zones(symbol, zones)
        else:
            self.zones.pop(symbol, None)
            await self.db.replace_zones(symbol, [])

    # ============================================================== proximity
    async def _prices(self) -> Dict[str, float]:
        if self.mark.prices and self.mark.stale_for < 30:
            return self.mark.prices
        try:
            data = await self.rest.mark_prices()
            self.rest_prices = {d["symbol"]: float(d["markPrice"]) for d in data if d.get("markPrice")}
        except Exception as exc:  # noqa: BLE001
            log.debug("rest price fallback failed: %s", exc)
        return self.rest_prices

    async def _scan_proximity(self) -> None:
        if self.paused:
            return
        prices = await self._prices()
        if not prices:
            return

        candidates = []
        for symbol, zones in self.zones.items():
            price = prices.get(symbol)
            if not price:
                continue
            if symbol in self.armed or self.monitor.has(symbol):
                continue
            if self.cooldown.get(symbol, 0) > time.time():
                continue
            for z in zones:
                if z.contains(price, self.cfg.arm_buffer_zone_frac):
                    candidates.append((z.score, symbol, z, price))
                    break

        candidates.sort(reverse=True, key=lambda c: c[0])
        if candidates:
            log.info("proximity scan: %d symbols inside a graded zone", len(candidates))

        for _, symbol, zone, price in candidates:
            if len(self.armed) >= self.cfg.max_armed_symbols:
                break
            if self.monitor.count >= self.cfg.max_active_trades:
                break
            await self._arm(symbol, zone, price)

    async def _arm(self, symbol: str, zone: Zone, price: float) -> None:
        async with self._arm_lock:
            if symbol in self.armed:
                return
            ctx = ArmedContext(symbol, zone, self.cfg,
                               tick_size=self.watchlist.tick_size(symbol),
                               decimals=self.watchlist.decimals(symbol),
                               ref_price=price)
            self.armed[symbol] = ctx
        try:
            await ctx.seed(self.rest)
            stream = SymbolStream(
                self.cfg.ws_base, symbol, ctx.on_event,
                depth_levels=self.cfg.depth_levels,
                depth_speed_ms=self.cfg.depth_speed_ms,
                intervals=[self.cfg.micro_interval, self.cfg.ltf_fast, self.cfg.ltf_slow],
            )
            await stream.start()
            self.streams[symbol] = stream
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to arm %s: %s", symbol, exc)
            await self._disarm(symbol, "arm failed")

    async def _disarm(self, symbol: str, reason: str) -> None:
        stream = self.streams.pop(symbol, None)
        if stream:
            await stream.stop()
        ctx = self.armed.pop(symbol, None)
        if ctx:
            ctx.dispose()
            log.info("disarmed %s (%s)", symbol, reason)

    # ============================================================ confirmation
    async def _evaluate_armed(self) -> None:
        for symbol in list(self.armed.keys()):
            ctx = self.armed.get(symbol)
            if ctx is None:
                continue

            if ctx.expired:
                await self._disarm(symbol, "arm window expired")
                self.cooldown[symbol] = time.time() + 900
                continue
            if not ctx.warm:
                continue
            if not ctx.still_in_range():
                await self._disarm(symbol, "price left the zone")
                self.cooldown[symbol] = time.time() + 600
                continue
            if self.paused or self.monitor.count >= self.cfg.max_active_trades:
                continue

            ctx.evaluations += 1
            self.evaluations += 1
            decision = evaluate(ctx, self.cfg, self.trend.get(symbol, "range"))
            ctx.last_eval = time.time()
            ctx.last_blockers = decision.blockers

            if not decision.passed:
                for b in decision.blockers:
                    self.blockers[b.split("(")[0]] += 1
                self.rejected += 1
                continue

            opposing = self.zone_engine.opposing_target(
                self.zones.get(symbol, []), ctx.direction, ctx.price)
            signal = build_signal(ctx, decision, self.cfg, opposing)
            if signal is None:
                for b in decision.blockers:
                    self.blockers[b.split("(")[0]] += 1
                self.rejected += 1
                continue

            await self._dispatch(signal, ctx)

    async def _dispatch(self, signal: Signal, ctx: ArmedContext) -> None:
        if await self.db.has_open_for(signal.symbol):
            await self._disarm(signal.symbol, "already has an open setup")
            return

        signal_id = await self.db.insert_signal(signal)
        await self.db.add_event(signal_id, "SIGNAL", signal.entry_ref,
                                f"{signal.grade} {signal.zone_score} conf={signal.confidence}")
        row = await self.db.signal(signal_id)
        if row:
            await self.monitor.add(row)

        fresh = " · fresh" if ctx.zone.touches == 0 else f" · tested {ctx.zone.touches}x"
        await self.bot.send(fmt.signal_message(signal, signal_id, fresh))

        self.signals_total += 1
        self.signals_today += 1
        await self.db.set_meta("signals_total", self.signals_total)
        log.info("SIGNAL #%d %s %s @ %.8f (conf %.2f)", signal_id, signal.symbol,
                 signal.direction, signal.entry_ref, signal.confidence)

        await self._disarm(signal.symbol, "signal fired")
        self.cooldown[signal.symbol] = time.time() + self.cfg.rearm_cooldown_minutes * 60

    # ============================================================ trade events
    async def _trade_event(self, trade: dict, kind: str, info: dict) -> None:
        if kind in ("TP1", "TP2"):
            await self.bot.send(fmt.tp_alert(trade, kind, info))
        else:
            await self.bot.send(fmt.close_alert(trade, kind, info))

    async def _release_symbol(self, symbol: str) -> None:
        """Setup finished - the coin goes back into the hunting pool."""
        self.cooldown[symbol] = time.time() + self.cfg.rearm_cooldown_minutes * 60
        try:
            await self._zones_for(symbol)
        except Exception as exc:  # noqa: BLE001
            log.debug("zone refresh after close failed for %s: %s", symbol, exc)

    # ================================================================= stats
    def _engine_stats(self) -> dict:
        zones = [z for zs in self.zones.values() for z in zs]
        return {
            "uptime": human_delta(time.time() - self.started_at),
            "paused": self.paused,
            "watchlist": len(self.watchlist.symbols),
            "zones": len(zones),
            "a_plus": len([z for z in zones if z.grade == "A+"]),
            "a_grade": len([z for z in zones if z.grade == "A"]),
            "armed": len(self.armed),
            "max_armed": self.cfg.max_armed_symbols,
            "signals_today": self.signals_today,
            "signals_total": self.signals_total,
            "cycles": self.cycles,
            "evaluations": self.evaluations,
            "rejected": self.rejected,
            "top_blockers": self.blockers.most_common(6),
            "min_volume": self.cfg.min_quote_volume,
            "zone_refresh": self.cfg.zone_refresh_hours,
            "proximity": self.cfg.proximity_interval_sec,
        }

    def _health(self) -> dict:
        size = 0.0
        try:
            size = os.path.getsize(self.cfg.db_path) / (1024 * 1024)
        except OSError:
            pass
        snap = self.limiter.snapshot()
        return {
            "uptime": human_delta(time.time() - self.started_at),
            "rss_mb": rss_mb(),
            "weight_used": snap["used_local"],
            "weight_reported": snap["used_reported"],
            "weight_budget": snap["budget"],
            "markprice_ok": self.mark.alive and self.mark.stale_for < 30,
            "markprice_symbols": len(self.mark.prices),
            "markprice_age": self.mark.stale_for if self.mark.last_msg_ts else 999,
            "flow_streams": len(self.streams),
            "reconnects": self.mark.reconnects + sum(s.reconnects for s in self.streams.values()),
            "db_path": self.cfg.db_path,
            "db_size_mb": size,
            "tasks": len([t for t in self._tasks if not t.done()]),
            "tg_sent": self.bot.sent,
        }

    # =============================================================== commands
    async def handle_command(self, cmd: str, args: List[str]) -> Optional[str]:
        prices = await self._prices()

        if cmd in ("start", "help"):
            return fmt.help_message()

        if cmd == "status":
            return fmt.status_message(self._engine_stats(), self.monitor.snapshot(prices))

        if cmd == "active":
            return fmt.active_message(self.monitor.snapshot(prices))

        if cmd == "watchlist":
            rows = await self.db.get_watchlist()
            nxt = max(0, self.cfg.watchlist_refresh_hours * 3600 -
                      (time.time() - self.watchlist.last_refresh))
            return fmt.watchlist_message(rows, self.cfg.min_quote_volume, human_delta(nxt))

        if cmd == "zones":
            if not args:
                top = sorted(self.zones.items(), key=lambda kv: -max(z.score for z in kv[1]))[:12]
                if not top:
                    return "No graded zones stored yet - the first scan may still be running."
                lines = ["🧭 <b>TOP ZONES</b> (use /zones SYMBOL for detail)"]
                for sym, zs in top:
                    best = max(zs, key=lambda z: z.score)
                    lines.append(f"{sym:<12} {best.grade} {best.score} · {best.kind} "
                                 f"{best.low:g}–{best.high:g}")
                return "\n".join(lines)
            symbol = args[0].upper()
            if not symbol.endswith(self.cfg.quote_asset):
                symbol += self.cfg.quote_asset
            rows = await self.db.get_zones(symbol)
            return fmt.zones_message(symbol, rows, prices.get(symbol))

        if cmd == "signals":
            n = int(args[0]) if args and args[0].isdigit() else 10
            return fmt.signals_message(await self.db.recent_signals(min(n, 30)))

        if cmd in ("pnl", "report"):
            period = (args[0].lower() if args else "all")
            since = _period_start(period)
            perf = await self.db.performance(since)
            breakdown = await self.db.symbol_breakdown(since)
            if cmd == "pnl":
                return fmt.pnl_message(perf, period, breakdown)
            return fmt.report_message(perf, period, breakdown, self._engine_stats())

        if cmd == "stats":
            return fmt.stats_message(self._engine_stats(),
                                     [c.stats() for c in self.armed.values()])

        if cmd == "health":
            return fmt.health_message(self._health())

        if cmd == "pause":
            self.paused = True
            return "⏸ Signal generation paused. Open setups are still monitored."

        if cmd == "resume":
            self.paused = False
            return "▶️ Signal generation resumed."

        if cmd == "close":
            if not args or not args[0].lstrip("#").isdigit():
                return "Usage: <code>/close 12</code>"
            sid = int(args[0].lstrip("#"))
            trade = self.monitor.active.get(sid)
            if not trade:
                return f"No open setup with id {sid}."
            price = prices.get(trade["symbol"], trade["entry_ref"])
            ok = await self.monitor.force_close(sid, price, "Closed manually via Telegram")
            return "Closed." if ok else "Could not close that setup."

        return f"Unknown command <code>/{cmd}</code>. Try /help."


def _batches(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i: i + size]


def _period_start(period: str) -> int:
    now = now_ms()
    day = 86400 * 1000
    return {
        "today": now - day,
        "day": now - day,
        "week": now - 7 * day,
        "month": now - 30 * day,
        "all": 0,
    }.get(period, 0)

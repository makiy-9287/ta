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
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from config import Settings
from notifier import formatter as fmt
from notifier.telegram import TelegramBot

from .armed import ArmedContext
from .confirm import evaluate
from .database import Database
from .events import DepthEvent, KlineEvent, TradeEvent
from .exchanges import ADAPTERS
from .feed import EventBus, PriceBook, SymbolRouter
from .indicators import htf_trend
from .liquidity import LiquidityMap
from .models import Signal, Zone
from .monitor import TradeMonitor
from .rate_limiter import WeightLimiter
from .risk import build_signal
from .utils import (collect_garbage, get_logger, human_delta, now_ms, rss_mb)
from .watchlist import WatchlistManager
from .ws import MarkPriceStream, SessionStream
from .zones import ZoneEngine

log = get_logger("engine")


class SniperEngine:
    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self.limiter = WeightLimiter(cfg.weight_budget, cfg.bulk_weight_share, name="binance")
        self.limiters = {"binance": self.limiter}

        # one adapter per configured venue; each gets its own budget because
        # the exchanges meter completely differently
        self.adapters: Dict[str, object] = {}
        for name in cfg.exchange_list:
            factory = ADAPTERS.get(name)
            if factory is None:
                log.warning("unknown exchange '%s' - ignoring", name)
                continue
            limiter = self.limiter if name == "binance" else \
                WeightLimiter(cfg.bybit_budget, cfg.bulk_weight_share, name=name)
            self.limiters[name] = limiter
            self.adapters[name] = factory(cfg, limiter)

        self.history = self.adapters.get(cfg.history_exchange) or \
            next(iter(self.adapters.values()))

        self.db = Database(cfg.db_path)
        self.bot = TelegramBot(cfg.telegram_token, cfg.telegram_chat_id,
                               cfg.telegram_poll_timeout, cfg.command_timeout_sec)
        self.watchlist = WatchlistManager(self.adapters, self.db, cfg)
        self.zone_engine = ZoneEngine(cfg)
        self.bus = EventBus(cfg.queue_shards, cfg.queue_maxsize, cfg.queue_batch)
        self.router = SymbolRouter(cfg)
        self.prices = PriceBook()
        self.mark = MarkPriceStream(cfg.ws_base, idle_timeout=cfg.ws_idle_timeout_sec) \
            if "binance" in self.adapters else None
        self.monitor = TradeMonitor(self.db, cfg, self._trade_event, self._release_symbol)

        self.zones: Dict[str, List[Zone]] = {}
        self.liquidity: Dict[str, LiquidityMap] = {}
        self.zone_ts: Dict[str, float] = {}
        self.trend: Dict[str, str] = {}
        self.armed: Dict[str, ArmedContext] = {}
        self.streams: Dict[str, SessionStream] = {}
        self.cooldown: Dict[str, float] = {}
        self.venue_trades: Counter = Counter()      # trade events actually received
        self.venue_starved: Counter = Counter()     # contexts that got nothing
        self.venue_blocked_until: Dict[str, float] = {}
        self.starve_count: Counter = Counter()      # per symbol, for backoff
        self.failed_zones: Dict[str, List[tuple]] = {}   # symbol -> (low, high, until)
        self.loss_streak: Counter = Counter()       # consecutive losses per symbol
        self.breaker_until = 0.0
        self._rest_price_ts = 0.0
        self._price_warn_ts = 0.0
        self.price_source = "starting"
        self._stale_warn_ts = 0.0

        self._arm_lock = asyncio.Lock()
        self._tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()

        self.paused = False
        self.asleep = False
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
        for adapter in self.adapters.values():
            await adapter.start()
        await self.bot.start()

        ok, name = await self.bot.verify()
        if not ok:
            raise RuntimeError(name)
        self.bot_name = name
        log.info("telegram connected as %s", name)

        self.signals_total = int(await self.db.get_meta("signals_total", 0) or 0)
        await self.monitor.load()
        await self._restore_cooldowns()

        # the queue workers start before any socket, so no event is ever
        # handled on the connection coroutine
        self.bus.start(self._consume)
        if self.mark:
            self.mark.start_hook = None
            await self.mark.start()

        log.info("building initial watchlist...")
        await self.watchlist.refresh()
        self._replan_streams()
        await self._rebuild_zones(initial=True)

        if self.cfg.startup_notice:
            await self.bot.send(fmt.startup_message(self._engine_stats(), self.bot_name))

        self._tasks = [
            asyncio.create_task(self._loop_watchlist(), name="watchlist"),
            asyncio.create_task(self._loop_zones(), name="zones"),
            asyncio.create_task(self._loop_proximity(), name="proximity"),
            asyncio.create_task(self._loop_arm(), name="arm"),
            asyncio.create_task(self._loop_monitor(), name="monitor"),
            asyncio.create_task(self._loop_flow_fallback(), name="flow-fallback"),
            asyncio.create_task(self._loop_prices(), name="prices"),
            asyncio.create_task(self._loop_schedule(), name="schedule"),
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
        await self.bus.stop()
        if self.mark:
            await self.mark.stop()
        await self.db.set_meta("signals_total", self.signals_total)
        for adapter in self.adapters.values():
            await adapter.close()
        await self.bot.close()
        await self.db.close()
        log.info("shutdown complete")

    # ================================================================== loops
    async def _restore_cooldowns(self) -> None:
        """In-memory cooldowns die with the process. Without this a symbol
        whose setup closed moments before a restart could be re-armed
        instantly, producing a duplicate signal on the same move."""
        try:
            recent = await self.db.last_signal_map()
        except Exception as exc:  # noqa: BLE001
            log.debug("could not restore cooldowns: %s", exc)
            return
        window = self.cfg.rearm_cooldown_minutes * 60
        now = time.time()
        restored = 0
        for symbol, ts in recent.items():
            until = ts / 1000.0 + window
            if until > now:
                self.cooldown[symbol] = until
                restored += 1
        if restored:
            log.info("restored re-arm cooldowns for %d symbols", restored)

    async def _loop_watchlist(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.sleep(self.cfg.watchlist_refresh_hours * 3600)
                await self.watchlist.refresh()
                self._replan_streams()
                if self.mark:
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
        """
        Roll through the watchlist continuously instead of rebuilding it all
        at once.

        A full rebuild of 130 symbols is ~1300 request weight. Fired as a
        burst it saturates the budget for two minutes and starves everything
        else. Zones move on the scale of hours, so refreshing the single
        stalest symbol every few seconds spreads exactly the same work into a
        trickle the budget never notices.
        """
        await asyncio.sleep(30)
        while not self._stop.is_set():
            await asyncio.sleep(self._zone_tick())
            try:
                symbol = self._stalest_symbol()
                if symbol:
                    await self._zones_for(symbol)
                    self.zone_ts[symbol] = time.time()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("rolling zone refresh failed: %s", exc)

    def _zone_tick(self) -> float:
        """Seconds between single-symbol refreshes to cover the whole list."""
        n = max(1, len(self.watchlist.symbols))
        return max(2.0, min(300.0, self.cfg.zone_refresh_hours * 3600.0 / n))

    def _stalest_symbol(self) -> Optional[str]:
        candidates = [s for s in self.watchlist.symbols
                      if s not in self.armed and not self.monitor.has(s)]
        if not candidates:
            return None
        return min(candidates, key=lambda s: self.zone_ts.get(s, 0.0))

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
                await self._prices()
                # only hand the monitor prices we know are current: a stale
                # quote means a stop that HAS been hit is never seen
                max_age = self.cfg.price_max_age_sec
                fresh = {s: p for s, p in self.prices.as_dict().items()
                         if self.prices.age(s) <= max_age}
                stale = [s for s in self.monitor.symbols() if s not in fresh]
                if stale and (time.time() - self._stale_warn_ts) > 120:
                    self._stale_warn_ts = time.time()
                    log.warning("no fresh price for open setups: %s - stop and target "
                                "detection is blind on these until the feed returns",
                                ", ".join(sorted(stale)[:6]))
                if fresh:
                    await self.monitor.on_prices(fresh)
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
                keep = set(self.watchlist.symbols) | set(self.monitor.symbols())
                if self.mark:
                    self.mark.prune(keep)
                self.prices.prune(keep)
                await self.db.housekeeping()
                freed = collect_garbage()
                today = time.strftime("%Y-%m-%d")
                if today != self._today:
                    self._today = today
                    self.signals_today = 0
                feed = self.bus.health()
                venues = " ".join(f"{v}={self.venue_trades[v]}" for v in self.adapters)
                blocked = [v for v in self.adapters
                           if self.venue_blocked_until.get(v, 0) > time.time()]
                log.info("housekeeping: %.0f MB rss, %d freed, armed=%d, trades by venue: %s%s, "
                         "queue pending=%d dropped=%d lag=%.0fms",
                         rss_mb(), freed, len(self.armed), venues,
                         f" (blocked: {','.join(blocked)})" if blocked else "",
                         feed["pending"], feed["dropped"], feed["lag_ms"])
                if self.mark and not self.mark.last_msg_ts \
                        and self.mark.silent_drops >= self.cfg.markprice_give_up:
                    # this host cannot receive the mark price stream. Every
                    # further attempt is noise: prices already come from REST
                    # and from the venues whose sockets do work.
                    log.warning("mark price stream abandoned after %d silent endpoints - "
                                "prices will come from REST and live trades",
                                self.mark.silent_drops)
                    await self.mark.stop()
                    self.mark = None
                elif self.mark and (not self.mark.alive or self.mark.stale_for > 120):
                    log.warning("mark price stream unhealthy (%s, %d reconnects, %d silent) "
                                "- restarting; prices are coming from %s",
                                self.mark.describe_staleness(), self.mark.reconnects,
                                self.mark.silent_drops, self.price_source)
                    await self.mark.stop()
                    await self.mark.start()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("housekeeping error: %s", exc)

    # ============================================================ zone building
    async def _rebuild_zones(self, initial: bool = False) -> None:
        """Initial pass over the whole watchlist, highest volume first."""
        symbols = list(self.watchlist.symbols)
        if not symbols:
            return
        log.info("building zones for %d symbols (paced against the bulk budget, "
                 "highest volume first)...", len(symbols))
        started = time.time()
        built = 0
        done = 0

        for batch in _batches(symbols, 4):
            if self._stop.is_set():
                return
            results = await asyncio.gather(*[self._zones_for(s) for s in batch],
                                           return_exceptions=True)
            for sym, res in zip(batch, results):
                done += 1
                self.zone_ts[sym] = time.time()
                if isinstance(res, Exception):
                    log.debug("zone build failed for %s: %s", sym, res)
                    continue
                built += 1
            if done % 25 == 0 or done == len(symbols):
                log.info("  zones: %d/%d symbols, %d graded so far (%.0fs elapsed)",
                         done, len(symbols), sum(len(z) for z in self.zones.values()),
                         time.time() - started)
            await asyncio.sleep(0.05)

        self.zone_build_ts = time.time()
        total = sum(len(z) for z in self.zones.values())
        log.info("zones ready: %d symbols, %d graded zones in %.0fs",
                 built, total, time.time() - started)
        if initial and total == 0:
            log.warning("no zones passed the %d point threshold on the first pass",
                        self.cfg.score_a)

    async def _zones_for(self, symbol: str) -> None:
        cfg = self.cfg
        if symbol in self.armed or self.monitor.has(symbol):
            return  # never move the goalposts mid-setup
        # background work: capped so it can never starve prices, arming or
        # the trade monitor of request weight
        htf = await self.history.klines(symbol, cfg.htf_interval, cfg.candle_limit, bulk=True)
        mtf = await self.history.klines(symbol, cfg.mtf_interval, cfg.candle_limit, bulk=True)
        if len(htf) < 100:
            return
        zones = self.zone_engine.build(symbol, htf, mtf)
        self.trend[symbol] = str(htf_trend(htf)["state"])
        if self.zone_engine.last_liquidity is not None:
            self.liquidity[symbol] = self.zone_engine.last_liquidity
        if zones:
            self.zones[symbol] = zones
            await self.db.replace_zones(symbol, zones)
        else:
            self.zones.pop(symbol, None)
            await self.db.replace_zones(symbol, [])

    # ============================================================== proximity
    async def _loop_prices(self) -> None:
        """
        Keep the price book current from every venue.

        Binance publishes an all-market mark price stream; Bybit does not, so
        its side is polled cheaply. Whichever arrives most recently wins, which
        also means one venue going dark never blinds us.
        """
        while not self._stop.is_set():
            try:
                if self.mark and self.mark.prices and self.mark.stale_for < 30:
                    self.prices.bulk_update(self.mark.prices, "binance-ws")
                    self.price_source = "websocket"
                else:
                    await self._poll_rest_prices()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("price loop error: %s", exc)
            await asyncio.sleep(self.cfg.price_poll_sec)

    async def _poll_rest_prices(self) -> None:
        now = time.time()
        if now - self._rest_price_ts < self.cfg.price_cache_sec:
            return
        got = False
        for name, adapter in self.adapters.items():
            if not hasattr(adapter, "prices"):
                continue
            try:
                prices = await asyncio.wait_for(adapter.prices(), timeout=30)
                if prices:
                    self.prices.bulk_update(prices, f"{name}-rest")
                    got = True
            except asyncio.TimeoutError:
                if now - self._price_warn_ts > 300:
                    self._price_warn_ts = now
                    log.warning("%s price poll timed out (weight %s)",
                                name, self.limiters[name].snapshot())
            except Exception as exc:  # noqa: BLE001
                log.debug("%s price poll failed: %s", name, exc)
        if got:
            self._rest_price_ts = now
            if self.price_source != "rest":
                log.warning("mark price stream unavailable - using REST prices")
            self.price_source = "rest"

    async def _prices(self) -> Dict[str, float]:
        """Snapshot of the merged price book; never blocks on the network."""
        if not len(self.prices):
            await self._poll_rest_prices()
        return self.prices.as_dict()

    async def _scan_proximity(self) -> None:
        if self.paused or self.asleep or self.breaker_until > time.time():
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
                if not z.contains(price, self.cfg.arm_buffer_zone_frac):
                    continue
                if not self._viable_under_trend(z, symbol):
                    self.blockers["skipped_unviable_vs_trend"] += 1
                    continue
                if self._zone_failed(symbol, z):
                    self.blockers["zone_already_failed"] += 1
                    continue
                candidates.append((z.score, symbol, z, price))
                break

        candidates.sort(reverse=True, key=lambda c: c[0])
        if candidates:
            log.info("proximity scan: %d symbols inside a graded zone", len(candidates))

        for _, symbol, zone, price in candidates:
            if len(self.armed) >= self._armed_capacity():
                break
            if self.monitor.count >= self.cfg.max_active_trades:
                break
            await self._arm(symbol, zone, price)

    async def _consume(self, event) -> None:
        """
        The only place market events are turned into analytics.

        Runs in a queue worker, never on a socket coroutine, so a burst of
        prints can never stall the connection that produced them.
        """
        symbol = getattr(event, "symbol", "")
        ctx = self.armed.get(symbol)
        if ctx is None:
            return
        if isinstance(event, TradeEvent):
            ctx.on_trade(event)
            self.venue_trades[event.exchange] += 1
            self.prices.update(symbol, event.price, f"{event.exchange}-trade")
        elif isinstance(event, DepthEvent):
            ctx.on_depth(event)
        elif isinstance(event, KlineEvent):
            ctx.on_kline(event)

    async def _arm(self, symbol: str, zone: Zone, price: float) -> None:
        venue = self.router.venue(symbol, self.cfg.history_exchange)
        if self.venue_blocked_until.get(venue, 0) > time.time():
            alternatives = [v for v in self.active_venues()
                            if symbol in self.watchlist.listings.get(v, set())]
            if alternatives:
                venue = alternatives[0]
        adapter = self.adapters.get(venue)
        if adapter is None:
            return

        async with self._arm_lock:
            if symbol in self.armed:
                return
            ctx = ArmedContext(symbol, zone, self.cfg,
                               tick_size=self.watchlist.tick_size(symbol),
                               decimals=self.watchlist.decimals(symbol),
                               ref_price=price, exchange=venue,
                               htf_liquidity=self.liquidity.get(symbol))
            self.armed[symbol] = ctx
        try:
            await ctx.seed(adapter, self.history)
            if not self._ws_usable():
                log.info("armed %s without a websocket (transport degraded, "
                         "using REST flow polling)", symbol)
                return
            session = adapter.flow_session(
                symbol,
                [self.cfg.micro_interval, self.cfg.ltf_fast,
                 self.cfg.ltf_slow, self.cfg.ltf_mid],
                self.cfg.depth_levels, self.cfg.depth_speed_ms)
            stream = SessionStream(session, self._publish, f"{venue}:{symbol}",
                                   idle_timeout=self.cfg.ws_flow_idle_timeout_sec)
            await stream.start()
            self.streams[symbol] = stream
        except Exception as exc:  # noqa: BLE001
            log.warning("failed to arm %s: %s", symbol, exc)
            await self._disarm(symbol, "arm failed")

    def _publish(self, events) -> None:
        """Socket-side callback: enqueue and return. No analytics here."""
        for ev in events:
            self.bus.publish(ev)

    def active_venues(self) -> List[str]:
        """Venues currently trusted to carry a live feed."""
        now = time.time()
        live = [v for v in self.adapters if self.venue_blocked_until.get(v, 0) < now]
        return live or list(self.adapters)

    def _replan_streams(self) -> None:
        self.router.plan(self.watchlist.symbols, self.watchlist.listings,
                         self.active_venues())

    def _note_starvation(self, symbol: str, venue: str) -> None:
        """
        A context that received no trades tells us something about the venue,
        not just the symbol.

        If one venue keeps starving its symbols while another is delivering,
        the network is blocking that venue's feed - re-arming the same symbols
        on it forever just burns request weight on a loop. Streaming is moved
        to the venue that works; the blocked one stays in use for REST history,
        which is a different port and usually unaffected.
        """
        self.venue_starved[venue] += 1
        self.starve_count[symbol] += 1
        healthy = [v for v in self.adapters
                   if v != venue and self.venue_trades[v] > 0
                   and self.venue_blocked_until.get(v, 0) < time.time()]
        if not healthy or self.venue_starved[venue] < self.cfg.venue_starve_threshold:
            return
        self.venue_blocked_until[venue] = time.time() + self.cfg.venue_block_minutes * 60
        self.venue_starved[venue] = 0
        log.warning("%s websocket feed delivered no trades for %d armed symbols while "
                    "%s is working - routing all streams to %s for %d minutes "
                    "(%s is still used for candle history over REST)",
                    venue, self.cfg.venue_starve_threshold, "/".join(healthy),
                    "/".join(healthy), self.cfg.venue_block_minutes, venue)
        self._replan_streams()

    def _viable_under_trend(self, zone: Zone, symbol: str) -> bool:
        """
        Would this setup be allowed to fire at all, given the 4H trend?

        Arming costs a full seed and holds a slot for ninety minutes. There is
        no point spending either on a zone the trend policy will refuse no
        matter how good the order flow turns out to be.
        """
        cfg = self.cfg
        trend = self.trend.get(symbol, "range")
        if zone.direction == "LONG":
            conflict = "hard" if trend == "strong_down" else ("soft" if trend == "down" else "none")
        else:
            conflict = "hard" if trend == "strong_up" else ("soft" if trend == "up" else "none")
        if conflict == "none":
            return True
        if conflict == "hard" and cfg.respect_htf_trend:
            return False
        if cfg.counter_trend_policy == "block":
            return False
        if cfg.counter_trend_policy == "strict" and zone.grade != "A+":
            return False
        return True

    # ================================================================ schedule
    def local_now(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(hours=self.cfg.tz_offset_hours)

    def sleep_state(self) -> tuple:
        """
        (asleep, human reason). Weekend books are thin and their sweeps rarely
        follow through, so the engine stops hunting and wakes on the first
        working morning. Open setups are still monitored while it sleeps.
        """
        cfg = self.cfg
        if not cfg.weekend_sleep:
            return False, ""
        days = cfg.sleep_day_set
        if not days:
            return False, ""
        now = self.local_now()
        weekday = now.weekday()
        if weekday in days:
            return True, now.strftime("%A")
        if ((weekday - 1) % 7) in days and now.hour < cfg.wake_hour:
            return True, f"{now.strftime('%A')} before {cfg.wake_hour:02d}:00"
        return False, ""

    def next_wake(self) -> datetime:
        """Next working morning - today's, if it has not passed yet."""
        now = self.local_now()
        probe = now.replace(hour=self.cfg.wake_hour, minute=0, second=0, microsecond=0)
        for _ in range(10):
            if probe > now and probe.weekday() not in self.cfg.sleep_day_set:
                return probe
            probe += timedelta(days=1)
        return probe

    async def _loop_schedule(self) -> None:
        asleep, _ = self.sleep_state()
        self.asleep = asleep
        while not self._stop.is_set():
            await asyncio.sleep(30)
            try:
                now_asleep, reason = self.sleep_state()
                if now_asleep == self.asleep:
                    continue
                self.asleep = now_asleep
                if now_asleep:
                    for sym in list(self.armed):
                        await self._disarm(sym, "weekend sleep")
                    wake = self.next_wake()
                    log.info("sleeping for the weekend (%s) - waking %s",
                             reason, wake.strftime("%A %H:%M"))
                    await self.bot.send(fmt.sleep_message(reason, wake,
                                                          self.monitor.count))
                else:
                    log.info("waking up - resuming the hunt")
                    await self.bot.send(fmt.wake_message(self._engine_stats()))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("schedule loop error: %s", exc)

    def _ws_usable(self) -> bool:
        """Has any websocket transport shown a sign of life?"""
        if self.bus.stats.pushed > 0:
            return True
        if self.mark is None:
            return True
        if self.mark.last_msg_ts:
            return True
        return self.mark.silent_drops < 3 and self.mark.reconnects < 5

    def _armed_capacity(self) -> int:
        """In REST-only mode each armed symbol costs real request weight, so
        the concurrent limit tightens automatically."""
        if self._ws_usable():
            return self.cfg.max_armed_symbols
        return min(self.cfg.max_armed_symbols, self.cfg.max_armed_fallback)

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
            try:
                await self._evaluate_one(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                # one bad symbol must never stop the others being evaluated
                log.exception("evaluation failed for %s: %s", symbol, exc)

    async def _evaluate_one(self, symbol: str) -> None:
        ctx = self.armed.get(symbol)
        if ctx is None:
            return

        if ctx.expired:
            await self._disarm(symbol, "arm window expired")
            self.cooldown[symbol] = time.time() + 900
            return
        if not ctx.warm:
            return
        if not ctx.still_in_range():
            await self._disarm(symbol, "price left the zone")
            self.cooldown[symbol] = time.time() + 600
            return

        # a context whose feed died and cannot be revived over REST is a
        # zombie: it holds memory and can only ever score frozen data
        if not ctx.flow_fresh(self.cfg.max_flow_age_sec * 3):
            venue = ctx.exchange
            await self._disarm(symbol, f"no flow data for {int(ctx.flow_age)}s")
            self._note_starvation(symbol, venue)
            # back off further each time rather than re-arming on a 12 minute
            # loop: every retry costs a full seed (trades, klines, depth)
            penalty = min(3600, 600 * (2 ** min(self.starve_count[symbol] - 1, 3)))
            self.cooldown[symbol] = time.time() + penalty
            return

        if self.paused or self.asleep or self.breaker_until > time.time() \
                or self.monitor.count >= self.cfg.max_active_trades:
            return

        ctx.evaluations += 1
        self.evaluations += 1
        decision = evaluate(ctx, self.cfg, self.trend.get(symbol, "range"))
        ctx.last_eval = time.time()
        ctx.last_blockers = decision.blockers

        if not decision.passed:
            self._record_blockers(decision.blockers)
            return

        opposing = self.zone_engine.opposing_target(
            self.zones.get(symbol, []), ctx.direction, ctx.price)
        signal = build_signal(ctx, decision, self.cfg, opposing)
        if signal is None:
            self._record_blockers(decision.blockers)
            return

        await self._dispatch(signal, ctx)

    def _record_blockers(self, blockers: List[str]) -> None:
        for b in blockers:
            self.blockers[b.split("(")[0]] += 1
        self.rejected += 1

    async def _loop_flow_fallback(self) -> None:
        """
        Keep armed symbols alive over REST when their WebSocket is silent.

        This is what lets the engine keep working on networks that accept the
        WS handshake and then deliver nothing - a surprisingly common situation
        on cloud hosts in restricted regions.
        """
        if not self.cfg.rest_fallback:
            return
        while not self._stop.is_set():
            await asyncio.sleep(self.cfg.flow_poll_sec)
            try:
                # Trigger on whichever clock is stale, not just the socket's.
                #
                # feed_age counts ANY event; flow_age counts trades only. A
                # stream delivering depth and klines but no trades looks alive
                # by the first measure and dead by the second - which is the
                # exact state that starves a context to death while the rescue
                # that exists for it never fires.
                idle = self.cfg.ws_flow_idle_timeout_sec
                stalled = [
                    (sym, ctx) for sym, ctx in list(self.armed.items())
                    if ctx.seeded and (ctx.feed_age > idle or ctx.flow_age > idle)
                ]
                if not stalled:
                    continue
                # oldest data first, capped so a dead network cannot burn the
                # whole weight budget on order-flow polling
                stalled.sort(key=lambda kv: -max(kv[1].flow_age, kv[1].feed_age))
                for symbol, ctx in stalled[: self.cfg.max_armed_fallback]:
                    if symbol not in self.armed:
                        continue
                    adapter = self.adapters.get(ctx.exchange) or self.history
                    got = await ctx.poll_rest(adapter, self.history)
                    log.info("REST flow poll %s via %s (ws idle %.0fs, trades idle %.0fs, "
                             "new trades: %s)", symbol, ctx.exchange, ctx.feed_age,
                             ctx.flow_age, "yes" if got else "none")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("flow fallback error: %s", exc)

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
        try:
            await self.bot.send(fmt.signal_message(signal, signal_id, fresh))
        except Exception as exc:  # noqa: BLE001
            # the setup is already persisted and monitored - a delivery failure
            # must not abort the disarm/cooldown bookkeeping below
            log.warning("could not deliver signal #%d: %s", signal_id, exc)

        self.signals_total += 1
        self.signals_today += 1
        await self.db.set_meta("signals_total", self.signals_total)
        log.info("SIGNAL #%d %s %s @ %.8f (conf %.2f)", signal_id, signal.symbol,
                 signal.direction, signal.entry_ref, signal.confidence)

        await self._disarm(signal.symbol, "signal fired")
        self.cooldown[signal.symbol] = time.time() + self.cfg.rearm_cooldown_minutes * 60

    # ============================================================ trade events
    async def _trade_event(self, trade: dict, kind: str, info: dict) -> None:
        if kind not in ("TP1", "TP2"):
            symbol = trade["symbol"]
            if kind == "LOSS":
                self.loss_streak[symbol] += 1
                low, high = trade.get("zone_low") or 0.0, trade.get("zone_high") or 0.0
                if low and high:
                    self._note_zone_failure(symbol, low, high)
                # each consecutive loss on a symbol buys a longer rest
                streak = self.loss_streak[symbol]
                extra = min(self.cfg.loss_backoff_max_minutes,
                            self.cfg.rearm_cooldown_minutes * (2 ** min(streak, 4)))
                self.cooldown[symbol] = time.time() + extra * 60
                if streak >= self.cfg.max_symbol_loss_streak:
                    log.warning("%s has lost %d in a row - resting it for %d minutes",
                                symbol, streak, extra)
            elif (trade.get("result_r") or 0) > 0.05:
                self.loss_streak[symbol] = 0
            await self._check_circuit_breaker()

        text = fmt.tp_alert(trade, kind, info) if kind in ("TP1", "TP2") \
            else fmt.close_alert(trade, kind, info)
        await self.bot.send(text)

    async def _check_circuit_breaker(self) -> None:
        """
        Stop hunting after a bad run.

        Without a brake the engine happily generated eighteen signals into a
        losing sequence. A rolling drawdown limit forces a pause so conditions
        can change before more risk is taken.
        """
        if self.cfg.max_drawdown_r <= 0 or self.breaker_until > time.time():
            return
        since = now_ms() - int(self.cfg.drawdown_window_hours * 3_600_000)
        try:
            perf = await self.db.performance(since)
        except Exception:  # noqa: BLE001
            return
        if perf["closed"] < self.cfg.drawdown_min_trades:
            return
        if perf["total_r"] > -abs(self.cfg.max_drawdown_r):
            return
        self.breaker_until = time.time() + self.cfg.breaker_cooldown_hours * 3600
        for sym in list(self.armed):
            await self._disarm(sym, "circuit breaker")
        log.warning("circuit breaker: %.2fR over %d trades in %sh - pausing for %sh",
                    perf["total_r"], perf["closed"], self.cfg.drawdown_window_hours,
                    self.cfg.breaker_cooldown_hours)
        await self.bot.send(fmt.breaker_message(perf, self.cfg.drawdown_window_hours,
                                                self.cfg.breaker_cooldown_hours))

    def _note_zone_failure(self, symbol: str, low: float, high: float) -> None:
        """
        A zone that just stopped a setup out is not a zone any more.

        Re-arming the same box minutes later is how one symbol produced three
        full losses in six hours: the level had already failed, and every
        subsequent sweep of it was continuation, not a reversal.
        """
        until = time.time() + self.cfg.zone_failure_minutes * 60
        entries = [z for z in self.failed_zones.get(symbol, []) if z[2] > time.time()]
        entries.append((low, high, until))
        self.failed_zones[symbol] = entries[-6:]

    def _zone_failed(self, symbol: str, zone: Zone) -> bool:
        now = time.time()
        for low, high, until in self.failed_zones.get(symbol, []):
            if until <= now:
                continue
            if min(zone.high, high) >= max(zone.low, low):     # overlaps
                return True
        return False

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
            "asleep": self.asleep,
            "sleep_reason": self.sleep_state()[1],
            "breaker": max(0, int((self.breaker_until - time.time()) / 60)),
            "next_wake": self.next_wake().strftime("%a %H:%M") if self.asleep else "",
            "watchlist": len(self.watchlist.symbols),
            "zones": len(zones),
            "a_plus": len([z for z in zones if z.grade == "A+"]),
            "a_grade": len([z for z in zones if z.grade == "A"]),
            "armed": len(self.armed),
            "max_armed": self._armed_capacity(),
            "signals_today": self.signals_today,
            "signals_total": self.signals_total,
            "cycles": self.cycles,
            "evaluations": self.evaluations,
            "rejected": self.rejected,
            "top_blockers": self.blockers.most_common(6),
            "min_volume": self.cfg.min_quote_volume,
            "exchanges": self.router.summary() or {e: 0 for e in self.adapters},
            "queue": self.bus.health(),
            "zone_refresh": self.cfg.zone_refresh_hours,
            "proximity": self.cfg.proximity_interval_sec,
        }

    def _health(self) -> dict:
        size = 0.0
        try:
            size = os.path.getsize(self.cfg.db_path) / (1024 * 1024)
        except OSError:
            pass
        feed = self.bus.health()
        venues = {}
        for name, lim in self.limiters.items():
            snap = lim.snapshot()
            venues[name] = {"used": snap["used_local"], "budget": snap["budget"],
                            "penalty": snap["penalty_sec"]}
        return {
            "uptime": human_delta(time.time() - self.started_at),
            "rss_mb": rss_mb(),
            "venues": venues,
            "weight_used": self.limiter.snapshot()["used_local"],
            "weight_reported": self.limiter.snapshot()["used_reported"],
            "weight_budget": self.limiter.snapshot()["budget"],
            "penalty_sec": self.limiter.snapshot()["penalty_sec"],
            "markprice_ok": bool(self.mark and self.mark.alive and self.mark.stale_for < 30),
            "markprice_symbols": len(self.mark.prices) if self.mark else 0,
            "markprice_age": self.mark.describe_staleness() if self.mark else "disabled",
            "price_source": self.price_source,
            "price_symbols": len(self.prices),
            "stale_prices": len([s for s in self.monitor.symbols()
                                 if self.prices.age(s) > self.cfg.price_max_age_sec]),
            "rest_price_symbols": len(self.prices),
            "flow_streams": len(self.streams),
            "assignment": self.router.summary(),
            "venue_trades": dict(self.venue_trades),
            "venue_blocked": [v for v in self.adapters
                              if self.venue_blocked_until.get(v, 0) > time.time()],
            "feed": feed,
            "reconnects": (self.mark.reconnects if self.mark else 0)
                          + sum(s.reconnects for s in self.streams.values()),
            "silent_sockets": (self.mark.silent_drops if self.mark else 0)
                              + sum(s.silent_drops for s in self.streams.values()),
            "rest_polls": sum(c.rest_polls for c in self.armed.values()),
            "weight_waits": self.limiter.snapshot()["waits"],
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
                populated = {s: zs for s, zs in self.zones.items() if zs}
                top = sorted(populated.items(), key=lambda kv: -max(z.score for z in kv[1]))[:12]
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

        if cmd == "why":
            if not args:
                armed = list(self.armed)
                return ("Usage: <code>/why BTCUSDT</code>\nArmed now: "
                        + (", ".join(armed) if armed else "nothing")) 
            symbol = args[0].upper()
            if not symbol.endswith(self.cfg.quote_asset):
                symbol += self.cfg.quote_asset
            ctx = self.armed.get(symbol)
            if ctx is None:
                return (f"{symbol} is not armed. Use /stats to see what is, "
                        f"or /zones {symbol} for its zone map.")
            decision = evaluate(ctx, self.cfg, self.trend.get(symbol, "range"))
            return fmt.why_message(ctx, decision, self.trend.get(symbol, "range"))

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

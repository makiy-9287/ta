"""
Active setup monitor.

Every signal is tracked from the moment it fires until it is finished -
TP3 or SL. On the way it reports TP1 and TP2, moves the stop to breakeven
after TP1 and to TP1 after TP2, and records MFE/MAE in R so the reports
mean something later.

Prices come from the single all-market mark price stream, so monitoring a
dozen open setups costs zero REST weight.
"""
from __future__ import annotations

import time
from typing import Awaitable, Callable, Dict, List, Optional

from .utils import fmt_price, get_logger, now_ms

log = get_logger("monitor")

Notify = Callable[[dict, str, dict], Awaitable[None]]
Released = Callable[[str], Awaitable[None]]


class TradeMonitor:
    @staticmethod
    def _fractions(cfg) -> tuple:
        f1 = max(0.0, min(1.0, cfg.tp1_fraction))
        f2 = max(0.0, min(1.0 - f1, cfg.tp2_fraction))
        return f1, f2, max(0.0, 1.0 - f1 - f2)

    def _realised_r(self, trade: dict, exit_price: float) -> float:
        """
        Result in R with scaled exits.

        Treating a setup as all-or-nothing is what turned eight TP1 hits into
        six zeros: price reached the first target, the stop moved to entry, and
        the whole position gave it back. Booking a share of the position at
        each target reflects how the setup is actually traded, so reaching TP1
        is worth something even when the remainder stops at breakeven.
        """
        entry = trade["entry_ref"]
        risk = abs(entry - trade["sl"]) or 1e-9
        long_ = trade["direction"] == "LONG"
        f1, f2, f3 = self._fractions(self.cfg)

        def r_of(price: float) -> float:
            return ((price - entry) if long_ else (entry - price)) / risk

        realised = 0.0
        remaining = 1.0
        for frac, key, stamp in ((f1, "tp1", "tp1_ts"), (f2, "tp2", "tp2_ts"),
                                 (f3, "tp3", "tp3_ts")):
            if trade.get(stamp) and frac > 0:
                realised += frac * r_of(trade[key])
                remaining -= frac
        if remaining > 1e-9:
            realised += remaining * r_of(exit_price)
        return realised

    def __init__(self, db, cfg, notify: Notify, on_release: Optional[Released] = None):
        self.db = db
        self.cfg = cfg
        self.notify = notify
        self.on_release = on_release
        self.active: Dict[int, dict] = {}

    async def load(self) -> None:
        for row in await self.db.active_signals():
            self.active[row["id"]] = dict(row)
        if self.active:
            log.info("resumed %d active setups from database", len(self.active))

    async def add(self, row: dict) -> None:
        self.active[row["id"]] = dict(row)

    @property
    def count(self) -> int:
        return len(self.active)

    def symbols(self) -> List[str]:
        return [t["symbol"] for t in self.active.values()]

    def has(self, symbol: str) -> bool:
        return any(t["symbol"] == symbol for t in self.active.values())

    # ------------------------------------------------------------------- tick
    async def on_prices(self, prices: Dict[str, float]) -> None:
        for tid in list(self.active.keys()):
            trade = self.active.get(tid)
            if not trade:
                continue
            price = prices.get(trade["symbol"])
            if not price:
                continue
            try:
                await self._evaluate(trade, price)
            except Exception as exc:  # noqa: BLE001
                log.exception("monitor error on %s: %s", trade["symbol"], exc)

    async def _notify_safe(self, trade: dict, kind: str, info: dict) -> None:
        """Alert delivery must never break the trade lifecycle - a Telegram
        outage cannot be allowed to strand a setup or keep its symbol out of
        the hunting pool."""
        try:
            await self.notify(trade, kind, info)
        except Exception as exc:  # noqa: BLE001
            log.warning("could not deliver %s alert for %s: %s", kind, trade["symbol"], exc)

    async def _evaluate(self, trade: dict, price: float) -> None:
        tid = trade["id"]
        long_ = trade["direction"] == "LONG"
        entry = trade["entry_ref"]
        risk = abs(entry - trade["sl"])
        if risk <= 0:
            return

        r_now = ((price - entry) if long_ else (entry - price)) / risk
        if r_now > (trade.get("mfe_r") or 0):
            trade["mfe_r"] = r_now
            await self.db.update_signal(tid, mfe_r=r_now)
        if r_now < (trade.get("mae_r") or 0):
            trade["mae_r"] = r_now
            await self.db.update_signal(tid, mae_r=r_now)

        stop = trade.get("sl_current") or trade["sl"]

        # ---------------------------------------------------------- stop hit
        if (long_ and price <= stop) or (not long_ and price >= stop):
            r = ((stop - entry) if long_ else (entry - stop)) / risk
            realised = self._realised_r(trade, stop)
            if trade.get("tp2_ts"):
                status, reason = "WIN", "Trailed stop after TP2"
            elif trade.get("tp1_ts"):
                status = "WIN" if realised > 0.05 else ("LOSS" if realised < -0.05 else "BREAKEVEN")
                reason = "Remainder stopped at breakeven after TP1" if abs(r) < 0.15 \
                    else "Stop after TP1"
            else:
                status, reason = "LOSS", "Stop loss hit"
            await self._close(trade, stop, status, reason, realised)
            return

        # ------------------------------------------------------------ targets
        for n, key in ((1, "tp1"), (2, "tp2"), (3, "tp3")):
            hit_field = f"tp{n}_ts"
            if trade.get(hit_field):
                continue
            target = trade[key]
            reached = price >= target if long_ else price <= target
            if not reached:
                continue

            trade[hit_field] = now_ms()
            updates = {hit_field: trade[hit_field], "status": f"TP{n}"}

            if n == 3:
                await self.db.update_signal(tid, **{hit_field: trade[hit_field]})
                realised = self._realised_r(trade, target)
                await self._close(trade, target, "WIN",
                                  "TP3 reached - setup complete", realised)
                return

            if n == 1 and self.cfg.breakeven_after_tp1:
                trade["sl_current"] = entry
                updates["sl_current"] = entry
            if n == 2 and self.cfg.trail_to_tp1_after_tp2:
                trade["sl_current"] = trade["tp1"]
                updates["sl_current"] = trade["tp1"]

            trade["status"] = f"TP{n}"
            await self.db.update_signal(tid, **updates)
            await self.db.add_event(tid, f"TP{n}", price, "")
            f1, f2, f3 = self._fractions(self.cfg)
            booked = (f1, f2, f3)[n - 1]
            await self._notify_safe(trade, f"TP{n}", {
                "price": price, "r": r_now, "new_stop": trade.get("sl_current"),
                "booked": booked, "locked_r": self._realised_r(trade, price)})
            log.info("%s #%d hit TP%d at %s", trade["symbol"], tid, n, fmt_price(price, trade["decimals"]))
            break

        # ------------------------------------------------------------ expiry
        age_h = (now_ms() - (trade["created_ts"] or now_ms())) / 3_600_000
        if age_h >= self.cfg.trade_ttl_hours and not trade.get("tp1_ts"):
            await self._close(trade, price, "EXPIRED", f"No progress in {int(age_h)}h",
                              self._realised_r(trade, price))

    # ------------------------------------------------------------------ close
    async def _close(self, trade: dict, price: float, status: str, reason: str, r: float) -> None:
        tid = trade["id"]
        entry = trade["entry_ref"]
        pct = ((price - entry) / entry) if trade["direction"] == "LONG" else ((entry - price) / entry)
        await self.db.update_signal(
            tid, status=status, closed_ts=now_ms(), close_price=price,
            close_reason=reason, result_r=round(r, 3), result_pct=round(pct, 5))
        await self.db.add_event(tid, status, price, reason)
        self.active.pop(tid, None)
        trade.update(status=status, close_price=price, close_reason=reason, result_r=r, result_pct=pct)
        await self._notify_safe(trade, status, {"price": price, "r": r, "pct": pct, "reason": reason})
        log.info("%s #%d closed %s (%.2fR)", trade["symbol"], tid, status, r)
        if self.on_release:
            try:
                await self.on_release(trade["symbol"])
            except Exception as exc:  # noqa: BLE001
                log.warning("release hook failed for %s: %s", trade["symbol"], exc)

    async def force_close(self, signal_id: int, price: float, reason: str = "Manual close") -> bool:
        trade = self.active.get(signal_id)
        if not trade:
            return False
        entry = trade["entry_ref"]
        risk = abs(entry - trade["sl"]) or 1e-9
        r = self._realised_r(trade, price)
        status = "WIN" if r > 0.05 else ("LOSS" if r < -0.05 else "BREAKEVEN")
        await self._close(trade, price, status, reason, r)
        return True

    def snapshot(self, prices: Dict[str, float]) -> List[dict]:
        out = []
        for t in self.active.values():
            price = prices.get(t["symbol"], 0.0)
            entry = t["entry_ref"]
            risk = abs(entry - t["sl"]) or 1e-9
            r = ((price - entry) if t["direction"] == "LONG" else (entry - price)) / risk if price else 0.0
            out.append({**t, "price": price, "r_now": r,
                        "age_h": (now_ms() - (t["created_ts"] or now_ms())) / 3_600_000})
        return sorted(out, key=lambda x: x["created_ts"])

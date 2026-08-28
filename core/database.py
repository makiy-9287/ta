"""
SQLite persistence.

Small, synchronous sqlite3 calls wrapped in asyncio.to_thread - the write
volume here is a few rows a day, so a connection pool would be theatre.
WAL mode keeps reads (Telegram reports) from blocking writes (monitor).
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

from .models import Signal, Zone
from .utils import get_logger, now_ms

log = get_logger("db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol       TEXT NOT NULL,
    direction    TEXT NOT NULL,
    grade        TEXT,
    zone_score   INTEGER,
    confidence   REAL,
    entry_low    REAL, entry_high REAL, entry_ref REAL,
    sl           REAL, sl_current REAL,
    tp1          REAL, tp2 REAL, tp3 REAL,
    risk_pct     REAL, rr REAL, decimals INTEGER,
    zone_low     REAL, zone_high REAL,
    status       TEXT DEFAULT 'ACTIVE',
    created_ts   INTEGER, closed_ts INTEGER,
    close_price  REAL, close_reason TEXT,
    tp1_ts INTEGER, tp2_ts INTEGER, tp3_ts INTEGER,
    mfe_r REAL DEFAULT 0, mae_r REAL DEFAULT 0,
    result_r REAL, result_pct REAL,
    reasons TEXT, meta TEXT
);
CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_ts);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id  INTEGER,
    ts         INTEGER,
    type       TEXT,
    price      REAL,
    note       TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_signal ON events(signal_id);

CREATE TABLE IF NOT EXISTS zones (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol     TEXT, kind TEXT, tf TEXT,
    low REAL, high REAL, score INTEGER, grade TEXT,
    touches INTEGER, members INTEGER,
    created_ts INTEGER, updated_ts INTEGER,
    meta TEXT
);
CREATE INDEX IF NOT EXISTS idx_zones_symbol ON zones(symbol);

CREATE TABLE IF NOT EXISTS watchlist (
    symbol       TEXT PRIMARY KEY,
    quote_volume REAL,
    price        REAL,
    updated_ts   INTEGER
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

CLOSED = ("WIN", "LOSS", "BREAKEVEN", "EXPIRED", "CANCELLED")


class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ setup
    def _connect(self) -> sqlite3.Connection:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(SCHEMA)
        conn.commit()
        return conn

    async def init(self) -> None:
        self._conn = await asyncio.to_thread(self._connect)
        log.info("database ready at %s", self.path)

    async def close(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    async def _run(self, fn, *args):
        async with self._lock:
            return await asyncio.to_thread(fn, *args)

    def _exec(self, sql: str, params: tuple = ()) -> int:
        cur = self._conn.execute(sql, params)
        self._conn.commit()
        return cur.lastrowid

    def _query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        cur = self._conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    async def execute(self, sql: str, params: tuple = ()) -> int:
        return await self._run(self._exec, sql, params)

    async def query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        return await self._run(self._query, sql, params)

    # -------------------------------------------------------------- watchlist
    async def save_watchlist(self, rows: List[Dict[str, Any]]) -> None:
        def _save():
            self._conn.execute("DELETE FROM watchlist")
            self._conn.executemany(
                "INSERT INTO watchlist(symbol, quote_volume, price, updated_ts) VALUES (?,?,?,?)",
                [(r["symbol"], r["quote_volume"], r.get("price", 0.0), now_ms()) for r in rows],
            )
            self._conn.commit()
        await self._run(_save)

    async def get_watchlist(self) -> List[Dict[str, Any]]:
        return await self.query("SELECT * FROM watchlist ORDER BY quote_volume DESC")

    # ------------------------------------------------------------------ zones
    async def replace_zones(self, symbol: str, zones: List[Zone]) -> None:
        def _replace():
            self._conn.execute("DELETE FROM zones WHERE symbol=?", (symbol,))
            self._conn.executemany(
                """INSERT INTO zones(symbol, kind, tf, low, high, score, grade, touches,
                                     members, created_ts, updated_ts, meta)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                [(z.symbol, z.kind, z.tf, z.low, z.high, z.score, z.grade, z.touches,
                  z.members, z.created_ts, now_ms(),
                  json.dumps({"breakdown": z.breakdown, "flags": _jsonable(z.flags)}))
                 for z in zones],
            )
            self._conn.commit()
        await self._run(_replace)

    async def get_zones(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        if symbol:
            return await self.query("SELECT * FROM zones WHERE symbol=? ORDER BY score DESC", (symbol,))
        return await self.query("SELECT * FROM zones ORDER BY score DESC")

    # ---------------------------------------------------------------- signals
    async def insert_signal(self, sig: Signal) -> int:
        sql = """INSERT INTO signals
            (symbol, direction, grade, zone_score, confidence, entry_low, entry_high, entry_ref,
             sl, sl_current, tp1, tp2, tp3, risk_pct, rr, decimals, zone_low, zone_high,
             status, created_ts, reasons, meta)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        params = (sig.symbol, sig.direction, sig.grade, sig.zone_score, sig.confidence,
                  sig.entry_low, sig.entry_high, sig.entry_ref, sig.sl, sig.sl,
                  sig.tp1, sig.tp2, sig.tp3, sig.risk_pct, sig.rr, sig.decimals,
                  sig.zone_low, sig.zone_high, "ACTIVE", sig.created_ts,
                  json.dumps(sig.reasons), json.dumps(_jsonable(sig.meta)))
        sig.id = await self.execute(sql, params)
        return sig.id

    async def active_signals(self) -> List[Dict[str, Any]]:
        return await self.query(
            "SELECT * FROM signals WHERE status NOT IN (%s) ORDER BY created_ts" %
            ",".join("?" * len(CLOSED)), tuple(CLOSED))

    async def signal(self, signal_id: int) -> Optional[Dict[str, Any]]:
        rows = await self.query("SELECT * FROM signals WHERE id=?", (signal_id,))
        return rows[0] if rows else None

    async def update_signal(self, signal_id: int, **fields) -> None:
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        await self.execute(f"UPDATE signals SET {cols} WHERE id=?", tuple(fields.values()) + (signal_id,))

    async def add_event(self, signal_id: int, type_: str, price: float, note: str = "") -> None:
        await self.execute(
            "INSERT INTO events(signal_id, ts, type, price, note) VALUES (?,?,?,?,?)",
            (signal_id, now_ms(), type_, price, note))

    async def events(self, signal_id: int) -> List[Dict[str, Any]]:
        return await self.query("SELECT * FROM events WHERE signal_id=? ORDER BY ts", (signal_id,))

    async def recent_signals(self, limit: int = 10) -> List[Dict[str, Any]]:
        return await self.query("SELECT * FROM signals ORDER BY created_ts DESC LIMIT ?", (limit,))

    async def has_open_for(self, symbol: str) -> bool:
        rows = await self.query(
            "SELECT id FROM signals WHERE symbol=? AND status NOT IN (%s) LIMIT 1" %
            ",".join("?" * len(CLOSED)), (symbol,) + tuple(CLOSED))
        return bool(rows)

    async def last_signal_ts(self, symbol: str) -> int:
        rows = await self.query(
            "SELECT MAX(COALESCE(closed_ts, created_ts)) AS t FROM signals WHERE symbol=?", (symbol,))
        return int(rows[0]["t"] or 0) if rows else 0

    # ------------------------------------------------------------------ stats
    async def performance(self, since_ms: int = 0) -> Dict[str, Any]:
        rows = await self.query(
            "SELECT * FROM signals WHERE status IN (%s) AND created_ts >= ?" %
            ",".join("?" * len(CLOSED)), tuple(CLOSED) + (since_ms,))
        wins = [r for r in rows if (r["result_r"] or 0) > 0.05]
        losses = [r for r in rows if (r["result_r"] or 0) < -0.05]
        flat = [r for r in rows if r not in wins and r not in losses]
        total_r = sum(r["result_r"] or 0 for r in rows)
        best = max(rows, key=lambda r: r["result_r"] or 0, default=None)
        worst = min(rows, key=lambda r: r["result_r"] or 0, default=None)
        tp1 = len([r for r in rows if r["tp1_ts"]])
        tp2 = len([r for r in rows if r["tp2_ts"]])
        tp3 = len([r for r in rows if r["tp3_ts"]])
        return {
            "closed": len(rows), "wins": len(wins), "losses": len(losses), "flat": len(flat),
            "winrate": (len(wins) / len(rows) * 100) if rows else 0.0,
            "total_r": total_r,
            "avg_r": (total_r / len(rows)) if rows else 0.0,
            "tp1_hits": tp1, "tp2_hits": tp2, "tp3_hits": tp3,
            "best": best, "worst": worst,
            "rows": rows,
        }

    async def symbol_breakdown(self, since_ms: int = 0, limit: int = 8) -> List[Dict[str, Any]]:
        return await self.query(
            """SELECT symbol, COUNT(*) AS n, SUM(result_r) AS r,
                      SUM(CASE WHEN result_r > 0 THEN 1 ELSE 0 END) AS wins
               FROM signals WHERE status IN (%s) AND created_ts >= ?
               GROUP BY symbol ORDER BY r DESC LIMIT ?""" % ",".join("?" * len(CLOSED)),
            tuple(CLOSED) + (since_ms, limit))

    # ------------------------------------------------------------------- meta
    async def set_meta(self, key: str, value: Any) -> None:
        await self.execute("INSERT INTO meta(key, value) VALUES (?,?) "
                           "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                           (key, json.dumps(value)))

    async def get_meta(self, key: str, default: Any = None) -> Any:
        rows = await self.query("SELECT value FROM meta WHERE key=?", (key,))
        if not rows:
            return default
        try:
            return json.loads(rows[0]["value"])
        except (ValueError, TypeError):
            return default

    # -------------------------------------------------------------- upkeep
    async def housekeeping(self, keep_days: int = 90) -> None:
        cutoff = now_ms() - keep_days * 86400 * 1000

        def _clean():
            self._conn.execute(
                "DELETE FROM events WHERE signal_id IN (SELECT id FROM signals WHERE closed_ts < ?)",
                (cutoff,))
            self._conn.execute("DELETE FROM signals WHERE closed_ts IS NOT NULL AND closed_ts < ?", (cutoff,))
            self._conn.commit()
        await self._run(_clean)


def _jsonable(obj: Any) -> Any:
    """Make nested dicts safe for json.dumps (tuples/sets/objects -> str)."""
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    return str(obj)

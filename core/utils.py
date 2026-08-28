"""Small shared helpers: logging, math, formatting, memory hygiene."""
from __future__ import annotations

import gc
import logging
import math
import os
import statistics
import sys
import time
from logging.handlers import RotatingFileHandler
from typing import Iterable, List, Optional, Sequence

_LOG_READY = False


def setup_logging(level: str = "INFO", log_file: Optional[str] = None) -> None:
    global _LOG_READY
    if _LOG_READY:
        return
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)-18s | %(message)s", "%Y-%m-%d %H:%M:%S")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=8 * 1024 * 1024, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(fh)

    for noisy in ("websockets.client", "websockets.protocol", "asyncio", "aiohttp.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    _LOG_READY = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


# ----------------------------------------------------------------- time helpers
def now_ms() -> int:
    return int(time.time() * 1000)


def now_s() -> float:
    return time.time()


def human_delta(seconds: float) -> str:
    seconds = int(max(0, seconds))
    d, r = divmod(seconds, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    if d:
        return f"{d}d {h}h"
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def ts_to_str(ms: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M", time.gmtime(ms / 1000))


# ----------------------------------------------------------------- math helpers
def safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def mean(vals: Sequence[float], default: float = 0.0) -> float:
    vals = [v for v in vals if v is not None]
    return statistics.fmean(vals) if vals else default


def median(vals: Sequence[float], default: float = 0.0) -> float:
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else default


def stdev(vals: Sequence[float], default: float = 0.0) -> float:
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return default
    try:
        return statistics.pstdev(vals)
    except statistics.StatisticsError:
        return default


def percentile(vals: Sequence[float], pct: float, default: float = 0.0) -> float:
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return default
    if len(vals) == 1:
        return vals[0]
    k = (len(vals) - 1) * max(0.0, min(1.0, pct))
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return vals[int(k)]
    return vals[lo] * (hi - k) + vals[hi] * (k - lo)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def pct_change(a: float, b: float) -> float:
    """Percentage move from a to b, as a fraction."""
    return 0.0 if a == 0 else (b - a) / a


def overlap(a_lo: float, a_hi: float, b_lo: float, b_hi: float) -> float:
    """Overlap size of two intervals (0 when they don't intersect)."""
    return max(0.0, min(a_hi, b_hi) - max(a_lo, b_lo))


def chunked(seq: Sequence, size: int) -> Iterable[List]:
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


# ---------------------------------------------------------------- formatting
def decimals_from_tick(tick: float) -> int:
    if tick <= 0:
        return 2
    s = f"{tick:.12f}".rstrip("0")
    if "." not in s:
        return 0
    return max(0, len(s.split(".")[1]))


def fmt_price(price: float, decimals: int = 4) -> str:
    if price is None:
        return "-"
    if price >= 1000:
        return f"{price:,.{min(decimals, 2)}f}"
    return f"{price:.{decimals}f}"


def fmt_pct(x: float, digits: int = 2) -> str:
    return f"{x * 100:+.{digits}f}%"


def fmt_usd(x: float) -> str:
    if x >= 1_000_000_000:
        return f"${x/1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"${x/1_000_000:.1f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


def esc(text: str) -> str:
    """Escape for Telegram HTML parse mode."""
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# ---------------------------------------------------------------- memory
def collect_garbage() -> int:
    """Explicit collection - matters on a long-lived VPS process."""
    return gc.collect()


def rss_mb() -> float:
    """Resident memory in MB, read straight from /proc (no psutil dependency)."""
    try:
        with open("/proc/self/statm", "r") as fh:
            pages = int(fh.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
    except Exception:
        return 0.0

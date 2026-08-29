"""
Detecting institutional execution algorithms in the tape.

A desk with size to move cannot lift the book - it would pay the spread all
the way up and advertise itself. So it slices: TWAP feeds a fixed clip at
fixed intervals, VWAP weights clips to volume, and an iceberg shows one small
lot while refilling the same price again and again.

Each leaves a distinct fingerprint:

    TWAP     one repeated clip size at metronomic intervals
    VWAP     clips scaling with volume, price hugging the VWAP line
    iceberg  executed volume at one price far exceeding the size ever
             displayed there, with repeated refills

This matters because it separates a real participant from a momentary flurry.
A spike in delta or absorption can evaporate in seconds; an algorithm working
an order is still there in five minutes, and it is the thing actually holding
the level.
"""
from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Dict, List, Optional, Sequence, Tuple

from .models import Candle
from .utils import clamp, get_logger, mean, median, stdev

log = get_logger("execalgo")


def _regularity(gaps: List[float]) -> float:
    """
    How metronomic a sequence of intervals is. 0 = perfect clockwork.

    Median absolute deviation, not standard deviation: a real tape sprinkles
    unrelated trades of the same size between an algorithm's clips, and a
    handful of those outliers destroys a mean-based statistic while leaving
    the underlying rhythm plainly visible to a median-based one.
    """
    if len(gaps) < 4:
        return 1.0
    med = median(gaps)
    if med <= 0:
        return 1.0
    mad = median([abs(g - med) for g in gaps])
    return mad / med


def _round_clip(qty: float) -> float:
    """Bucket sizes so 0.501 and 0.499 count as the same clip."""
    if qty <= 0:
        return 0.0
    exp = math.floor(math.log10(qty))
    step = 10 ** (exp - 1)
    return round(round(qty / step) * step, 10)


def detect_slicing(trades: Sequence[Tuple[int, float, float, bool]],
                   side: Optional[bool] = None,
                   min_clips: int = 12, regularity_max: float = 0.75) -> Dict[str, object]:
    """
    `trades` is a sequence of (ts_ms, price, qty, buy).

    Returns evidence of a sliced order: the dominant clip, how much of the
    flow it represents, and how metronomic its arrival is.
    """
    res: Dict[str, object] = {"found": False, "twap": False, "clip": 0.0, "count": 0,
                              "share": 0.0, "regularity": 1.0, "period": 0.0, "side": ""}
    rows = [t for t in trades if side is None or t[3] == side]
    if len(rows) < min_clips * 2:
        return res

    # Group by clip size in ONE pass, then score every candidate.
    #
    # Ranking candidates by frequency first was wrong: a busy tape produces
    # dozens of noisy size buckets, any of which can outnumber an algorithm
    # quietly working a few dozen clips underneath. Frequency is not the
    # signal - rhythm is - so every size with enough prints gets measured.
    groups: Dict[float, List[Tuple[int, float, bool]]] = defaultdict(list)
    total_vol = 0.0
    for ts, price, qty, buy in rows:
        if qty <= 0:
            continue
        total_vol += qty
        groups[_round_clip(qty)].append((ts, qty, buy))
    if not groups or total_vol <= 0:
        return res

    best = None
    for clip, items in groups.items():
        if clip <= 0 or len(items) < min_clips:
            continue
        gaps = [(b[0] - a[0]) / 1000.0 for a, b in zip(items, items[1:]) if b[0] > a[0]]
        gaps = [g for g in gaps if g > 0]
        if len(gaps) < 4:
            continue
        regularity = _regularity(gaps)
        key = (regularity, -len(items))
        if best is None or key < best["key"]:
            buys = sum(1 for it in items if it[2])
            best = {"key": key, "clip": clip, "count": len(items),
                    "regularity": regularity, "period": round(median(gaps), 2),
                    "share": sum(it[1] for it in items) / total_vol,
                    "side": "buy" if buys > len(items) / 2 else "sell"}

    if best is None:
        return res

    res.update(found=True, clip=best["clip"], count=best["count"],
               share=round(best["share"], 3), regularity=round(best["regularity"], 3),
               period=best["period"], side=best["side"],
               twap=bool(best["regularity"] <= regularity_max and best["count"] >= min_clips))
    return res


def detect_iceberg(trades: Sequence[Tuple[int, float, float, bool]],
                   heatmap, grid: float, min_ratio: float = 2.5,
                   min_executions: int = 8) -> Dict[str, object]:
    """
    Executed volume at a price far exceeding the size ever *shown* there.

    That gap is the tell: the book displayed 20 lots, 300 traded, price did not
    move through. Someone is refilling behind the scenes.
    """
    res: Dict[str, object] = {"found": False, "price": 0.0, "executed": 0.0,
                              "displayed": 0.0, "ratio": 0.0, "side": "", "refills": 0}
    if not trades or grid <= 0:
        return res

    by_level: Dict[float, List[float]] = defaultdict(list)
    sides: Dict[float, List[bool]] = defaultdict(list)
    for ts, price, qty, buy in trades:
        key = round(math.floor(price / grid) * grid, 10)
        by_level[key].append(qty)
        sides[key].append(buy)
    if not by_level:
        return res

    best = None
    for level, qtys in by_level.items():
        if len(qtys) < min_executions:
            continue
        executed = sum(qtys)
        buy_share = sum(sides[level]) / len(sides[level])
        # the resting side being consumed is the opposite of the aggressor
        side = "bid" if buy_share < 0.5 else "ask"
        store = heatmap.bids if side == "bid" else heatmap.asks
        bucket = store.get(level) if store else None
        displayed = bucket.peak_size if bucket else 0.0
        refills = bucket.refills if bucket else 0
        if displayed <= 0:
            continue
        ratio = executed / displayed
        if ratio >= min_ratio and (best is None or ratio > best[1]):
            best = (level, ratio, executed, displayed, side, refills)

    if best:
        level, ratio, executed, displayed, side, refills = best
        res.update(found=True, price=level, executed=round(executed, 3),
                   displayed=round(displayed, 3), ratio=round(ratio, 2),
                   side=side, refills=refills)
    return res


def session_vwap(candles: Sequence[Candle], lookback: int = 240) -> Dict[str, float]:
    """Rolling VWAP with one standard-deviation bands."""
    window = list(candles[-lookback:]) if len(candles) > lookback else list(candles)
    if not window:
        return {}
    num = den = 0.0
    typicals: List[float] = []
    weights: List[float] = []
    for c in window:
        typical = (c.high + c.low + c.close) / 3.0
        num += typical * c.volume
        den += c.volume
        typicals.append(typical)
        weights.append(c.volume)
    if den <= 0:
        return {}
    vwap = num / den
    var = sum(w * (t - vwap) ** 2 for t, w in zip(typicals, weights)) / den
    sd = math.sqrt(max(0.0, var))
    return {"vwap": vwap, "sd": sd, "upper": vwap + sd, "lower": vwap - sd,
            "upper2": vwap + 2 * sd, "lower2": vwap - 2 * sd}


def vwap_context(candles: Sequence[Candle], price: float, direction: str,
                 lookback: int = 240) -> Dict[str, object]:
    """
    Where price sits against VWAP, and whether that favours the trade.

    Institutions benchmark against VWAP: buying below it is a good fill, so a
    long from beneath VWAP is trading with that incentive rather than against
    it.
    """
    v = session_vwap(candles, lookback)
    res: Dict[str, object] = {"available": False}
    if not v or v["sd"] <= 0:
        return res
    z = (price - v["vwap"]) / v["sd"]
    if direction == "LONG":
        favourable = z <= 0.6
        stretched = z <= -2.0
    else:
        favourable = z >= -0.6
        stretched = z >= 2.0
    res.update(available=True, vwap=v["vwap"], z=round(z, 2),
               favourable=bool(favourable), stretched=bool(stretched),
               upper=v["upper"], lower=v["lower"])
    return res


def analyse_execution(book, heatmap, direction: str, window_sec: int = 900,
                      cfg=None) -> Dict[str, object]:
    """Full institutional-participation read for one armed symbol."""
    trades = book.recent_trades(window_sec)
    passive_side = False if direction == "LONG" else True   # aggressor we expect to be absorbed
    min_clips = getattr(cfg, "algo_min_clips", 12)
    regularity_max = getattr(cfg, "algo_regularity_max", 0.75)

    slicing = detect_slicing(trades, side=None, min_clips=min_clips,
                             regularity_max=regularity_max)
    opposing = detect_slicing(trades, side=passive_side, min_clips=min_clips,
                              regularity_max=regularity_max)
    iceberg = detect_iceberg(trades, heatmap, book.grid,
                             min_ratio=getattr(cfg, "iceberg_ratio", 2.5))

    want_side = "bid" if direction == "LONG" else "ask"
    supportive_iceberg = bool(iceberg["found"] and iceberg["side"] == want_side)

    return {
        "slicing": slicing,
        "opposing_slicing": opposing,
        "iceberg": iceberg,
        "supportive_iceberg": supportive_iceberg,
        "institutional": bool(supportive_iceberg or (slicing["twap"] and slicing["count"] >= min_clips)),
    }

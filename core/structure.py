"""
Market structure on the lower timeframes.

Three questions, in order:

  1. Did price take out obvious liquidity (a prior swing low/high)?   -> sweep
  2. Did it come back and hold the other side of that level?          -> reclaim
  3. Did it then break the local structure in our direction?          -> MSS

No sweep, no trade. That is the rule the whole model rests on.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence

from .indicators import atr, swing_points
from .models import Candle, Zone
from .utils import get_logger

log = get_logger("structure")


def detect_sweep(candles: Sequence[Candle], direction: str, cfg,
                 zone: Optional[Zone] = None, atr_val: float = 0.0,
                 liquidity: Optional[object] = None) -> Dict[str, object]:
    """
    Find the most recent liquidity sweep of a prior swing point.

    Recency is the whole point. A sweep 30 bars ago is stale: that liquidity is
    already taken, the level no longer protects anything, and price will slice
    straight back through it on the next drive. Only a sweep inside the last
    `sweep_max_age_bars` counts, and the wick of *that* sweep - not some older
    one - is what the stop is placed beyond.
    """
    res: Dict[str, object] = {"found": False, "reclaimed": False}
    n = len(candles)
    if n < 20:
        return res

    a = atr_val or atr(candles, 14)
    if a <= 0:
        return res

    look = min(cfg.sweep_lookback_bars, n - 3)
    highs, lows = swing_points(candles, 2, 2)
    pivots = lows if direction == "LONG" else highs
    pivots = [(i, lvl) for i, lvl in pivots if i >= n - look and i <= n - 3]
    if not pivots:
        return res

    if zone is not None:
        pad = 0.35 * zone.height
        if direction == "LONG":
            pivots = [(i, l) for i, l in pivots if l <= zone.high + pad]
        else:
            pivots = [(i, l) for i, l in pivots if l >= zone.low - pad]
    if not pivots:
        return res

    best: Optional[Dict[str, object]] = None
    for i, level in reversed(pivots):
        pierce = max(a * cfg.sweep_min_pierce_atr, level * 0.0003)
        if direction == "LONG":
            pierced = [j for j in range(i + 1, n) if candles[j].low < level - pierce]
        else:
            pierced = [j for j in range(i + 1, n) if candles[j].high > level + pierce]
        if not pierced:
            continue

        first_pierce, last_pierce = pierced[0], pierced[-1]
        age = n - 1 - last_pierce
        if age > cfg.sweep_max_age_bars:
            continue

        # the extreme of THIS sweep only: bars from the last excursion beyond
        # the level, never an older penetration further back
        recent_window = candles[max(first_pierce, last_pierce - cfg.sweep_wick_bars):]
        if direction == "LONG":
            extreme = min(c.low for c in recent_window)
            wick_bar = min(recent_window, key=lambda c: c.low)
            reclaimed = candles[-1].close > level and any(c.close > level for c in candles[last_pierce:])
            displacement = (candles[-1].close - extreme) / a
        else:
            extreme = max(c.high for c in recent_window)
            wick_bar = max(recent_window, key=lambda c: c.high)
            reclaimed = candles[-1].close < level and any(c.close < level for c in candles[last_pierce:])
            displacement = (extreme - candles[-1].close) / a

        # was there anything actually resting at the swept level?
        structural = None
        if liquidity is not None:
            pool = _matching_level(liquidity, direction, level, a)
            structural = pool.to_dict() if pool is not None else None

        candidate = {
            "found": True,
            "level": level,
            "pivot_idx": i,
            "sweep_idx": first_pierce,
            "last_pierce_idx": last_pierce,
            "extreme": extreme,
            "wick_ts": wick_bar.ts,
            "age_bars": age,
            "fresh": bool(age <= cfg.sweep_fresh_bars),
            "reclaimed": bool(reclaimed),
            "displacement_atr": round(displacement, 2),
            "pierce_atr": round(abs(level - extreme) / a, 3),
            "structural": structural,
        }
        # prefer the freshest reclaimed sweep
        if best is None or (candidate["reclaimed"] and not best["reclaimed"]) or \
                (candidate["reclaimed"] == best["reclaimed"] and age < best["age_bars"]):
            best = candidate
        if best and best["reclaimed"] and best["age_bars"] <= cfg.sweep_fresh_bars:
            break

    return best or res


def _matching_level(liquidity, direction: str, level: float, atr_val: float):
    """Find the structural pool that the sweep took out, if any."""
    levels = liquidity.lows if direction == "LONG" else liquidity.highs
    tol = max(atr_val * 0.35, level * 0.0008)
    matches = [l for l in levels if abs(l.price - level) <= tol]
    return max(matches, key=lambda l: l.strength) if matches else None


def detect_mss(candles: Sequence[Candle], direction: str, cfg,
               after_idx: Optional[int] = None) -> Dict[str, object]:
    """
    Market structure shift: close through the last opposing swing point that
    formed on the way into the sweep.
    """
    res: Dict[str, object] = {"found": False}
    n = len(candles)
    if n < 15:
        return res

    pivot_idx = after_idx if after_idx is not None else n - 1
    lo_bound = max(0, pivot_idx - cfg.mss_lookback_bars)
    window = candles[lo_bound: pivot_idx + 1]
    if len(window) < 6:
        return res

    highs, lows = swing_points(window, 2, 2)
    ref = highs if direction == "LONG" else lows
    if not ref:
        # fall back to the extreme of the approach leg
        level = max(c.high for c in window) if direction == "LONG" else min(c.low for c in window)
    else:
        level = ref[-1][1]

    close = candles[-1].close
    if direction == "LONG" and close > level:
        res.update(found=True, level=level, close=close)
    elif direction == "SHORT" and close < level:
        res.update(found=True, level=level, close=close)
    else:
        res.update(found=False, level=level, close=close)
    return res


def reclaim_strength(candles: Sequence[Candle], direction: str, level: float,
                     atr_val: float) -> float:
    """How convincingly price is holding the reclaimed level (in ATR)."""
    if atr_val <= 0 or not candles:
        return 0.0
    close = candles[-1].close
    return (close - level) / atr_val if direction == "LONG" else (level - close) / atr_val


def structure_bias(candles: Sequence[Candle], lookback: int = 40) -> str:
    """Quick HH/HL vs LH/LL read on the lower timeframe."""
    window = list(candles[-lookback:]) if len(candles) > lookback else list(candles)
    highs, lows = swing_points(window, 2, 2)
    if len(highs) < 2 or len(lows) < 2:
        return "neutral"
    hh = highs[-1][1] > highs[-2][1]
    hl = lows[-1][1] > lows[-2][1]
    lh = highs[-1][1] < highs[-2][1]
    ll = lows[-1][1] < lows[-2][1]
    if hh and hl:
        return "bullish"
    if lh and ll:
        return "bearish"
    return "neutral"

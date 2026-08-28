"""Pure-python technical primitives. No numpy needed - keeps the VPS image light."""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from .models import Candle
from .utils import mean, median, percentile, stdev


def true_range(prev_close: float, c: Candle) -> float:
    return max(c.high - c.low, abs(c.high - prev_close), abs(c.low - prev_close))


def atr(candles: Sequence[Candle], period: int = 14) -> float:
    if len(candles) < 2:
        return 0.0
    trs = [true_range(candles[i - 1].close, candles[i]) for i in range(1, len(candles))]
    window = trs[-period:] if len(trs) >= period else trs
    return mean(window)


def atr_series(candles: Sequence[Candle], period: int = 14) -> List[float]:
    """Wilder-ish rolling ATR aligned to candles (index 0 = 0.0)."""
    out = [0.0] * len(candles)
    if len(candles) < 2:
        return out
    trs = [0.0] + [true_range(candles[i - 1].close, candles[i]) for i in range(1, len(candles))]
    run = 0.0
    for i in range(1, len(candles)):
        if i <= period:
            run = mean(trs[1 : i + 1])
        else:
            run = (run * (period - 1) + trs[i]) / period
        out[i] = run
    return out


def ema(values: Sequence[float], period: int) -> List[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


# ------------------------------------------------------------------- structure
def find_pivots(candles: Sequence[Candle], left: int = 3, right: int = 3) -> Tuple[List[int], List[int]]:
    """
    Fractal pivots. Returns (pivot_high_indexes, pivot_low_indexes).

    Strict against the left side, tolerant of equality on the right, so a
    flat double-bottom still registers instead of silently disappearing -
    equal lows are exactly the liquidity we are hunting.
    """
    highs: List[int] = []
    lows: List[int] = []
    n = len(candles)
    for i in range(left, n - right):
        h, l = candles[i].high, candles[i].low
        is_high = True
        is_low = True
        for j in range(i - left, i):
            if candles[j].high >= h:
                is_high = False
            if candles[j].low <= l:
                is_low = False
            if not is_high and not is_low:
                break
        if is_high or is_low:
            for j in range(i + 1, i + right + 1):
                if candles[j].high > h:
                    is_high = False
                if candles[j].low < l:
                    is_low = False
                if not is_high and not is_low:
                    break
        if is_high:
            highs.append(i)
        if is_low:
            lows.append(i)
    return highs, lows


def swing_points(candles: Sequence[Candle], left: int = 2, right: int = 2) -> Tuple[List[Tuple[int, float]], List[Tuple[int, float]]]:
    hi_idx, lo_idx = find_pivots(candles, left, right)
    return ([(i, candles[i].high) for i in hi_idx], [(i, candles[i].low) for i in lo_idx])


def htf_trend(candles: Sequence[Candle]) -> Dict[str, object]:
    """
    Classify the higher-timeframe trend.
    Returns dict(state, strength) where state in
    {strong_up, up, range, down, strong_down}.
    """
    if len(candles) < 60:
        return {"state": "range", "strength": 0.0}
    closes = [c.close for c in candles]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    e200 = ema(closes, 200)[-1] if len(closes) >= 200 else e50
    price = closes[-1]

    hi, lo = swing_points(candles[-90:], 3, 3)
    hh = len(hi) >= 2 and hi[-1][1] > hi[-2][1]
    hl = len(lo) >= 2 and lo[-1][1] > lo[-2][1]
    lh = len(hi) >= 2 and hi[-1][1] < hi[-2][1]
    ll = len(lo) >= 2 and lo[-1][1] < lo[-2][1]

    score = 0
    score += 1 if e20 > e50 else -1
    score += 1 if price > e50 else -1
    score += 1 if e50 > e200 else -1
    score += 1 if (hh and hl) else 0
    score -= 1 if (lh and ll) else 0

    spread = abs(e20 - e50) / max(1e-12, e50)
    if score >= 3:
        state = "strong_up" if spread > 0.012 else "up"
    elif score <= -3:
        state = "strong_down" if spread > 0.012 else "down"
    elif score >= 1:
        state = "up"
    elif score <= -1:
        state = "down"
    else:
        state = "range"
    return {"state": state, "strength": round(score / 5.0, 2), "ema20": e20, "ema50": e50}


def range_position(candles: Sequence[Candle], price: float, lookback: int = 120) -> float:
    """Where a price sits inside the recent range: 0 = range low, 1 = range high."""
    window = candles[-lookback:] if len(candles) > lookback else candles
    if not window:
        return 0.5
    hi = max(c.high for c in window)
    lo = min(c.low for c in window)
    if hi - lo <= 0:
        return 0.5
    return (price - lo) / (hi - lo)


# ------------------------------------------------------------------------- CVD
def cvd_series(candles: Sequence[Candle]) -> List[float]:
    """Cumulative volume delta built from kline taker-buy volume."""
    out: List[float] = []
    run = 0.0
    for c in candles:
        run += c.delta
        out.append(run)
    return out


def cvd_divergence(candles: Sequence[Candle], direction: str, lookback: int = 60,
                   pivot: int = 2) -> Dict[str, object]:
    """
    Classic price vs CVD divergence.

    LONG  : price lower-low  while CVD higher-low  -> selling pressure fading.
    SHORT : price higher-high while CVD lower-high -> buying pressure fading.
    """
    window = list(candles[-lookback:]) if len(candles) > lookback else list(candles)
    res = {"found": False, "type": "", "strength": 0.0}
    if len(window) < (pivot * 2 + 6):
        return res
    cvd = cvd_series(window)
    highs, lows = swing_points(window, pivot, pivot)

    if direction == "LONG":
        if len(lows) < 2:
            return res
        (i1, p1), (i2, p2) = lows[-2], lows[-1]
        if p2 < p1 and cvd[i2] > cvd[i1]:
            scale = max(1e-9, abs(cvd[i1]) + abs(cvd[i2]))
            res.update(found=True, type="bullish", strength=min(1.0, (cvd[i2] - cvd[i1]) / scale))
    else:
        if len(highs) < 2:
            return res
        (i1, p1), (i2, p2) = highs[-2], highs[-1]
        if p2 > p1 and cvd[i2] < cvd[i1]:
            scale = max(1e-9, abs(cvd[i1]) + abs(cvd[i2]))
            res.update(found=True, type="bearish", strength=min(1.0, (cvd[i1] - cvd[i2]) / scale))
    return res


def cvd_reclaim(candles: Sequence[Candle], direction: str, lookback: int = 40,
                recovery_frac: float = 0.35) -> Dict[str, object]:
    """
    CVD hits an extreme, then recovers a meaningful share of that excursion
    while price holds. This is the 'reclaim' half of divergence/reclaim.
    """
    window = list(candles[-lookback:]) if len(candles) > lookback else list(candles)
    res = {"found": False, "recovered": 0.0}
    if len(window) < 12:
        return res
    cvd = cvd_series(window)
    start = cvd[0]
    if direction == "LONG":
        trough = min(cvd)
        ti = cvd.index(trough)
        if ti >= len(cvd) - 2:
            return res
        drop = start - trough
        if drop <= 0:
            return res
        rec = (cvd[-1] - trough) / drop
        if rec >= recovery_frac and window[-1].close > window[ti].close:
            res.update(found=True, recovered=round(rec, 3))
    else:
        peak = max(cvd)
        pi = cvd.index(peak)
        if pi >= len(cvd) - 2:
            return res
        rise = peak - start
        if rise <= 0:
            return res
        rec = (peak - cvd[-1]) / rise
        if rec >= recovery_frac and window[-1].close < window[pi].close:
            res.update(found=True, recovered=round(rec, 3))
    return res


def delta_zscore(candles: Sequence[Candle], lookback: int = 60) -> float:
    window = candles[-lookback:] if len(candles) > lookback else candles
    if len(window) < 10:
        return 0.0
    deltas = [c.delta for c in window]
    sd = stdev(deltas[:-1])
    if sd <= 0:
        return 0.0
    return (deltas[-1] - mean(deltas[:-1])) / sd


def volume_profile_poc(candles: Sequence[Candle], bins: int = 30) -> Optional[float]:
    """Rough point-of-control from candle midpoints (used for range context)."""
    if not candles:
        return None
    hi = max(c.high for c in candles)
    lo = min(c.low for c in candles)
    if hi <= lo:
        return None
    step = (hi - lo) / bins
    buckets = [0.0] * bins
    for c in candles:
        idx = min(bins - 1, max(0, int((((c.high + c.low) / 2) - lo) / step)))
        buckets[idx] += c.volume
    best = buckets.index(max(buckets))
    return lo + step * (best + 0.5)

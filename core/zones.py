"""
Support / Resistance engine.

Two stages:

  1. Detection  - fractal pivots -> ATR-tolerance clustering -> zone boxes.
  2. Scoring    - the 100-point rubric:

        A. Higher-timeframe confluence ....... 30
        B. Reaction quality .................. 20
        C. Liquidity ......................... 20
        D. Volume / order-flow history ....... 20
        E. Freshness ......................... 10

     80-100 -> A+   |   70-79 -> A   |   below 70 -> discarded.

Out of ~100 candidate levels on a chart this typically leaves 3-8 zones.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .indicators import atr, cvd_divergence, find_pivots, range_position
from .liquidity import LiquidityMap, StructuralLevel
from .models import Candle, Zone
from .utils import get_logger, median, overlap, percentile

log = get_logger("zones")


@dataclass
class ZoneContext:
    candles: List[Candle]
    atr: float
    med_volume: float
    p80_volume: float
    p20_delta: float
    p80_delta: float
    pivot_highs: List[int]
    pivot_lows: List[int]


def build_context(candles: Sequence[Candle], left: int, right: int) -> ZoneContext:
    cl = list(candles)
    ph, pl = find_pivots(cl, left, right)
    vols = [c.volume for c in cl]
    deltas = [c.delta for c in cl]
    return ZoneContext(
        candles=cl,
        atr=atr(cl, 14) or (cl[-1].close * 0.01 if cl else 0.0),
        med_volume=median(vols),
        p80_volume=percentile(vols, 0.80),
        p20_delta=percentile(deltas, 0.20),
        p80_delta=percentile(deltas, 0.80),
        pivot_highs=ph,
        pivot_lows=pl,
    )


# --------------------------------------------------------------------- stage 1
def _cluster(indexes: List[int], levels: List[float], tol: float) -> List[List[int]]:
    """Group pivot indexes whose levels sit within `tol` of the group's mean."""
    if not indexes:
        return []
    order = sorted(range(len(indexes)), key=lambda i: levels[i])
    groups: List[List[int]] = []
    cur = [order[0]]
    cur_ref = levels[order[0]]
    for oi in order[1:]:
        if abs(levels[oi] - cur_ref) <= tol:
            cur.append(oi)
            cur_ref = sum(levels[i] for i in cur) / len(cur)
        else:
            groups.append(cur)
            cur = [oi]
            cur_ref = levels[oi]
    groups.append(cur)
    return [[indexes[i] for i in g] for g in groups]


def detect_zones(ctx: ZoneContext, symbol: str, tf: str, cfg) -> List[Zone]:
    """Turn pivot clusters into demand/supply boxes."""
    out: List[Zone] = []
    if len(ctx.candles) < 60 or ctx.atr <= 0:
        return out

    tol = ctx.atr * cfg.cluster_atr_tol
    min_w = ctx.atr * cfg.zone_min_atr_width
    max_w = ctx.atr * cfg.zone_max_atr_width

    for kind, idxs in (("demand", ctx.pivot_lows), ("supply", ctx.pivot_highs)):
        levels = [ctx.candles[i].low if kind == "demand" else ctx.candles[i].high for i in idxs]
        for group in _cluster(idxs, levels, tol):
            members = [ctx.candles[i] for i in group]
            if kind == "demand":
                lo = min(c.low for c in members)
                hi = max(c.body_low for c in members)
            else:
                hi = max(c.high for c in members)
                lo = min(c.body_high for c in members)

            if hi <= lo:
                mid = (hi + lo) / 2
                lo, hi = mid - min_w / 2, mid + min_w / 2
            if hi - lo < min_w:
                pad = (min_w - (hi - lo)) / 2
                lo, hi = lo - pad, hi + pad
            if hi - lo > max_w:
                if kind == "demand":
                    hi = lo + max_w
                else:
                    lo = hi - max_w

            out.append(Zone(
                symbol=symbol, kind=kind, tf=tf, low=lo, high=hi,
                created_ts=ctx.candles[max(group)].ts,
                members=len(group),
                flags={"formation_idx": max(group), "first_idx": min(group)},
            ))
    return out


def structural_zones(liq: LiquidityMap, symbol: str, tf: str, ctx: ZoneContext,
                     cfg) -> List[Zone]:
    """
    Turn major swing points into zones.

    A prior HH, HL, LL or LH is a support/resistance level in its own right:
    price reversed there, so orders rest there. Those are precisely the pockets
    institutions drive price into - through the level to trigger the cluster,
    then away. Pivot clustering alone misses the isolated but structurally
    decisive swing, so we add them explicitly.
    """
    out: List[Zone] = []
    min_w = ctx.atr * cfg.zone_min_atr_width
    candles = ctx.candles

    for level in liq.highs + liq.lows:
        if level.label not in ("HH", "HL", "LH", "LL"):
            continue
        if level.strength < cfg.structural_min_strength:
            continue
        if level.index >= len(candles):
            continue
        c = candles[level.index]
        kind = "demand" if level.kind == "low" else "supply"
        if kind == "demand":
            lo, hi = c.low, max(c.body_low, c.low + min_w)
        else:
            hi, lo = c.high, min(c.body_high, c.high - min_w)
        if hi - lo < min_w:
            pad = (min_w - (hi - lo)) / 2
            lo, hi = lo - pad, hi + pad

        out.append(Zone(
            symbol=symbol, kind=kind, tf=tf, low=lo, high=hi,
            created_ts=c.ts, members=level.equal_count,
            flags={"formation_idx": level.index, "first_idx": level.index,
                   "structural": level.label, "structural_strength": level.strength,
                   "swept": level.swept, "source": "structure"},
        ))
    return out


def absorb_structural(pivot_zones: List[Zone], structural: List[Zone]) -> List[Zone]:
    """
    Fold structural levels into the zone map without distorting it.

    When a labelled swing sits inside a zone the clustering already found, they
    describe the same level - so the existing box is *tagged* rather than
    widened. Widening it would inflate the touch count and wrongly age a fresh
    zone. Only structurally decisive swings that clustering missed entirely are
    added as new zones.
    """
    out = list(pivot_zones)
    for s in structural:
        twin = next((z for z in out if z.kind == s.kind and z.overlaps(s)), None)
        if twin is not None:
            twin.flags.setdefault("structural", s.flags.get("structural"))
            twin.flags["structural_strength"] = max(
                float(twin.flags.get("structural_strength") or 0),
                float(s.flags.get("structural_strength") or 0))
            twin.members = max(twin.members, s.members)
            continue
        out.append(s)
    return out


def _merge_overlaps(zones: List[Zone]) -> List[Zone]:
    """Fold zones that overlap heavily into one box (keeps the map readable)."""
    merged: List[Zone] = []
    for z in sorted(zones, key=lambda x: x.low):
        if merged and merged[-1].kind == z.kind:
            prev = merged[-1]
            ov = overlap(prev.low, prev.high, z.low, z.high)
            if ov > 0 and ov >= 0.5 * min(prev.height, z.height):
                prev.low = min(prev.low, z.low)
                prev.high = max(prev.high, z.high)
                prev.members += z.members
                prev.created_ts = max(prev.created_ts, z.created_ts)
                prev.flags["formation_idx"] = max(prev.flags.get("formation_idx", 0),
                                                  z.flags.get("formation_idx", 0))
                continue
        merged.append(z)
    return merged


# --------------------------------------------------------------------- stage 2
def _count_tests(zone: Zone, candles: Sequence[Candle]) -> Tuple[int, int, bool]:
    """
    Tests after formation, timestamp of the last one, and whether the zone
    has been decisively broken (two closes clean through it).
    """
    start = int(zone.flags.get("formation_idx", 0)) + 1
    tests = 0
    last_ts = 0
    in_test = False
    broke = 0
    fail_level = zone.low - 0.3 * zone.height if zone.kind == "demand" else zone.high + 0.3 * zone.height

    for c in candles[start:]:
        touching = c.low <= zone.high and c.high >= zone.low
        if touching and not in_test:
            tests += 1
            last_ts = c.ts
            in_test = True
        elif not touching:
            in_test = False
        if (zone.kind == "demand" and c.close < fail_level) or \
           (zone.kind == "supply" and c.close > fail_level):
            broke += 1
        else:
            broke = 0
        if broke >= 2:
            return tests, last_ts, True
    return tests, last_ts, False


def _score_reaction(zone: Zone, ctx: ZoneContext) -> Tuple[int, Dict[str, bool]]:
    """B. Reaction quality - 20 pts (rejection wick 10 + displacement 10)."""
    pts = 0
    flags = {"rejection": False, "displacement": False}
    idx = int(zone.flags.get("formation_idx", 0))
    candles = ctx.candles
    if idx >= len(candles):
        return 0, flags

    window = candles[max(0, idx - 1): idx + 2]
    for c in window:
        rng = c.range
        if zone.kind == "demand":
            if c.lower_wick >= 0.45 * rng and c.close > c.body_low:
                flags["rejection"] = True
        else:
            if c.upper_wick >= 0.45 * rng and c.close < c.body_high:
                flags["rejection"] = True
    if flags["rejection"]:
        pts += 10

    after = candles[idx + 1: idx + 5]
    if after and ctx.atr > 0:
        if zone.kind == "demand":
            move = max(c.high for c in after) - zone.high
        else:
            move = zone.low - min(c.low for c in after)
        impulsive = any(c.range >= 1.5 * ctx.atr and c.body_ratio >= 0.55 and
                        (c.bullish if zone.kind == "demand" else not c.bullish) for c in after)
        if move >= 1.6 * ctx.atr or impulsive:
            flags["displacement"] = True
            pts += 10
    return pts, flags


def _score_liquidity_map(zone: Zone, ctx: ZoneContext, liq: LiquidityMap
                         ) -> Tuple[int, Dict[str, object]]:
    """
    C. Liquidity - 20 pts, judged against the structural map.

    swing/structural level in front  +5
    equal highs or lows              +5
    untapped liquidity to sweep     +10
    """
    pts = 0
    flags: Dict[str, object] = {"swing_in_front": False, "equal_levels": False,
                                "sweep_potential": False, "front_label": ""}
    reach = 2.5 * ctx.atr
    if zone.kind == "demand":
        near = [l for l in liq.lows if zone.low - reach <= l.price <= zone.high + reach * 0.4]
    else:
        near = [l for l in liq.highs if zone.low - reach * 0.4 <= l.price <= zone.high + reach]

    if near:
        flags["swing_in_front"] = True
        best = max(near, key=lambda l: l.strength)
        flags["front_label"] = best.label
        pts += 5

    if any(l.equal_count >= 2 for l in near):
        flags["equal_levels"] = True
        pts += 5

    untapped = [l for l in near if not l.swept]
    if untapped:
        flags["sweep_potential"] = True
        flags["untapped_level"] = round(max(untapped, key=lambda l: l.strength).price, 10)
        pts += 10
    return pts, flags


def _score_liquidity(zone: Zone, ctx: ZoneContext) -> Tuple[int, Dict[str, bool]]:
    """C. Liquidity - 20 pts (swings in front 5 + equal highs/lows 5 + sweep potential 10)."""
    pts = 0
    flags = {"swing_in_front": False, "equal_levels": False, "sweep_potential": False}
    candles = ctx.candles
    idx = int(zone.flags.get("formation_idx", 0))
    reach = 2.5 * ctx.atr

    if zone.kind == "demand":
        pivots = [(i, candles[i].low) for i in ctx.pivot_lows if zone.low - reach <= candles[i].low <= zone.high]
    else:
        pivots = [(i, candles[i].high) for i in ctx.pivot_highs if zone.low <= candles[i].high <= zone.high + reach]

    if pivots:
        flags["swing_in_front"] = True
        pts += 5

    eq_tol = max(ctx.atr * 0.12, (zone.mid or 1) * 0.0006)
    levels = sorted(p[1] for p in pivots)
    for a, b in zip(levels, levels[1:]):
        if abs(a - b) <= eq_tol:
            flags["equal_levels"] = True
            pts += 5
            break

    # untapped liquidity beyond the zone = fuel for the sweep we want to trade
    for i, lvl in pivots:
        after = candles[i + 1:]
        if not after:
            continue
        if zone.kind == "demand" and lvl <= zone.high:
            if min(c.low for c in after) >= lvl - ctx.atr * 0.02 and i < idx + 6:
                flags["sweep_potential"] = True
                break
        if zone.kind == "supply" and lvl >= zone.low:
            if max(c.high for c in after) <= lvl + ctx.atr * 0.02 and i < idx + 6:
                flags["sweep_potential"] = True
                break
    if flags["sweep_potential"]:
        pts += 10
    return pts, flags


def _score_flow_history(zone: Zone, ctx: ZoneContext) -> Tuple[int, Dict[str, bool]]:
    """D. Volume / order-flow history - 20 pts (4 x 5)."""
    pts = 0
    flags = {"volume": False, "absorption": False, "delta_reversal": False, "cvd_div": False}
    candles = ctx.candles
    idx = int(zone.flags.get("formation_idx", 0))
    window_idx = [i for i in range(max(0, idx - 2), min(len(candles), idx + 3))]
    zone_candles = [candles[i] for i in window_idx]
    if not zone_candles:
        return 0, flags

    if ctx.med_volume > 0 and max(c.volume for c in zone_candles) >= 1.3 * ctx.med_volume:
        flags["volume"] = True
        pts += 5

    for c in zone_candles:
        big_vol = ctx.med_volume > 0 and c.volume >= 1.8 * ctx.med_volume
        tight = ctx.atr > 0 and c.range <= 1.05 * ctx.atr
        held = (c.close >= c.low + 0.55 * c.range) if zone.kind == "demand" else (c.close <= c.high - 0.55 * c.range)
        if big_vol and tight and held:
            flags["absorption"] = True
            pts += 5
            break

    for i in window_idx:
        c = candles[i]
        extreme = c.delta <= ctx.p20_delta if zone.kind == "demand" else c.delta >= ctx.p80_delta
        if not extreme:
            continue
        follow = candles[i + 1: i + 4]
        if not follow:
            continue
        reclaimed = (max(x.close for x in follow) > zone.high) if zone.kind == "demand" \
            else (min(x.close for x in follow) < zone.low)
        if reclaimed:
            flags["delta_reversal"] = True
            pts += 5
            break

    div = cvd_divergence(candles[max(0, idx - 40): idx + 4],
                         "LONG" if zone.kind == "demand" else "SHORT", lookback=44, pivot=2)
    if div.get("found"):
        flags["cvd_div"] = True
        pts += 5
    return pts, flags


def score_zone(zone: Zone, ctx: ZoneContext, mtf_zones: List[Zone], cfg,
               liq: Optional[LiquidityMap] = None) -> Zone:
    breakdown: Dict[str, int] = {}
    flags: Dict[str, object] = dict(zone.flags)

    # --- A. higher-timeframe confluence (30) -------------------------------
    structural = zone.flags.get("structural")
    # a labelled swing only counts as "major" when it is genuinely decisive -
    # otherwise every HL on the chart is handed the full 20 points and the
    # zone map floods
    strong_structure = float(zone.flags.get("structural_strength") or 0) >= cfg.structural_major_strength
    major = zone.members >= 2 or (bool(structural) and strong_structure)
    a_pts = 20 if major else 12
    flags["htf_major"] = major
    mtf_hit = any(z.kind == zone.kind and z.overlaps(zone) for z in mtf_zones)
    if mtf_hit:
        a_pts += 10
    flags["mtf_overlap"] = mtf_hit
    breakdown["htf_confluence"] = a_pts

    # --- B. reaction quality (20) ------------------------------------------
    b_pts, b_flags = _score_reaction(zone, ctx)
    breakdown["reaction"] = b_pts
    flags.update(b_flags)

    # --- C. liquidity (20) --------------------------------------------------
    if liq is not None:
        c_pts, c_flags = _score_liquidity_map(zone, ctx, liq)
    else:
        c_pts, c_flags = _score_liquidity(zone, ctx)
    breakdown["liquidity"] = c_pts
    flags.update(c_flags)

    # --- D. volume / order flow history (20) --------------------------------
    d_pts, d_flags = _score_flow_history(zone, ctx)
    breakdown["flow_history"] = d_pts
    flags.update(d_flags)

    # --- E. freshness (10) --------------------------------------------------
    tests, last_ts, broken = _count_tests(zone, ctx.candles)
    zone.touches = tests
    zone.last_test_ts = last_ts
    flags["broken"] = broken
    e_pts = 10 if tests == 0 else (5 if tests == 1 else 0)
    breakdown["freshness"] = e_pts

    zone.score = min(100, sum(breakdown.values()))
    zone.grade = "A+" if zone.score >= cfg.score_a_plus else ("A" if zone.score >= cfg.score_a else "")
    zone.breakdown = breakdown
    zone.flags = flags
    return zone


# --------------------------------------------------------------------- facade
class ZoneEngine:
    def __init__(self, cfg):
        self.cfg = cfg
        self.last_liquidity: Optional[LiquidityMap] = None

    def build(self, symbol: str, htf: Sequence[Candle], mtf: Sequence[Candle]) -> List[Zone]:
        cfg = self.cfg
        if len(htf) < 80:
            return []

        htf_ctx = build_context(htf, cfg.pivot_left, cfg.pivot_right)
        mtf_ctx = build_context(mtf, cfg.pivot_left, cfg.pivot_right) if len(mtf) >= 80 else None

        htf_liq = LiquidityMap(htf, left=cfg.pivot_left, right=cfg.pivot_right,
                               equal_tol_atr=cfg.equal_level_atr_tol)
        self.last_liquidity = htf_liq
        pivot_zones = _merge_overlaps(detect_zones(htf_ctx, symbol, cfg.htf_interval, cfg))
        htf_zones = absorb_structural(
            pivot_zones, structural_zones(htf_liq, symbol, cfg.htf_interval, htf_ctx, cfg))
        mtf_zones = _merge_overlaps(detect_zones(mtf_ctx, symbol, cfg.mtf_interval, cfg)) if mtf_ctx else []

        price = htf[-1].close
        scored: List[Zone] = []
        for z in htf_zones:
            z = score_zone(z, htf_ctx, mtf_zones, cfg, liq=htf_liq)

            if z.flags.get("broken"):
                continue
            if z.touches >= cfg.max_zone_tests:
                continue
            if not z.grade:
                continue

            # a zone parked in the middle of the range is a coin flip - drop it
            pos = range_position(htf, z.mid, 120)
            z.flags["range_pos"] = round(pos, 3)
            if z.kind == "demand" and pos > cfg.range_position_guard:
                continue
            if z.kind == "supply" and pos < (1.0 - cfg.range_position_guard):
                continue

            # ignore zones on the wrong side of price (already consumed)
            if z.kind == "demand" and price < z.low:
                continue
            if z.kind == "supply" and price > z.high:
                continue

            # carry the higher-timeframe volatility with the zone: it is the
            # yardstick for how far price can travel in a session, and the
            # risk model has no 4H candles of its own
            z.flags["htf_atr"] = htf_ctx.atr
            z.flags["mtf_partner"] = next(
                ((m.low, m.high) for m in mtf_zones if m.kind == z.kind and m.overlaps(z)), None)
            scored.append(z)

        scored.sort(key=lambda x: (-x.score, x.distance_frac(price)))
        return scored[: self.cfg.max_zones_per_symbol]

    @staticmethod
    def opposing_target(zones: Sequence[Zone], direction: str, price: float) -> Optional[float]:
        """Nearest opposing zone edge - used to cap TP3 at real structure."""
        if direction == "LONG":
            cands = [z.low for z in zones if z.kind == "supply" and z.low > price]
            return min(cands) if cands else None
        cands = [z.high for z in zones if z.kind == "demand" and z.high < price]
        return max(cands) if cands else None

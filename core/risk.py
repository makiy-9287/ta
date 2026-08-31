"""
Turning a confirmed setup into levels.

Stop placement follows one rule: it goes just beyond the wick of the sweep
that *just* happened. Not below an older sweep thirty bars back - that
liquidity has already been taken, the level protects nothing, and price will
cut straight through it on the next drive. The recent wick is the price at
which the setup is genuinely wrong.

Targets follow the mirror rule: they go at liquidity, not at round R
multiples. Price travels toward resting size - the heatmap shelf, the untapped
swing high, the opposing zone - so that is where profit is taken. R multiples
remain as a fallback when the map is empty, and every target must still clear
a minimum reward for the risk taken.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .confirm import liquidity_targets
from .indicators import atr
from .models import Decision, Signal
from .utils import clamp, get_logger, now_ms

log = get_logger("risk")


def _pick_target(targets: List[dict], entry: float, risk: float, direction: str,
                 min_r: float, max_r: float, used: List[float], merge_pct: float,
                 reach: float, prefer_strength: bool = False) -> Optional[dict]:
    """
    Best liquidity pool inside a REWARD WINDOW.

    A minimum alone is not enough. With only a floor, the first pool clearing
    it wins however far away it sits - which is how a target lands 119R out,
    at a price the market would need to double to reach. Each target now has a
    ceiling too, and an absolute reach limit derived from recent volatility, so
    a level has to be both worth taking and plausibly reachable.

    TP1 and TP2 take the NEAREST qualifying pool, because their job is to be
    hit - TP1 in particular buys the breakeven stop. TP3 takes the STRONGEST
    pool in its window, because its job is to be where the move actually ends.
    """
    window: List[dict] = []
    for t in targets:
        level = float(t["price"])
        move = (level - entry) if direction == "LONG" else (entry - level)
        if move <= 0 or (reach > 0 and move > reach):
            continue
        r = move / risk
        if r < min_r or r > max_r:
            continue
        if any(abs(level - u) / max(entry, 1e-9) < merge_pct for u in used):
            continue
        window.append({**t, "r": r, "move": move})

    if not window:
        return None
    if prefer_strength:
        window.sort(key=lambda t: (-float(t.get("strength") or 0), t["move"]))
    else:
        window.sort(key=lambda t: t["move"])
    return window[0]


def build_signal(ctx, decision: Decision, cfg,
                 opposing_level: Optional[float] = None) -> Optional[Signal]:
    direction = ctx.direction
    long_ = direction == "LONG"
    price = ctx.price
    sweep = decision.details.get("sweep") or {}
    fast = ctx.candles.get(cfg.ltf_fast) or []
    slow = ctx.candles.get(cfg.ltf_slow) or []
    a = atr(fast, 14) or atr(slow, 14)
    if a <= 0 or price <= 0:
        return None

    if not sweep.get("found"):
        decision.block("no_sweep_for_stop_placement")
        return None

    # ------------------------------------------------------------- stop loss
    # the wick of the RECENT sweep, never an older one
    extreme = float(sweep.get("extreme") or (price - a))
    level = float(sweep.get("level") or price)
    age = int(sweep.get("age_bars") or 0)
    if age > cfg.sweep_max_age_bars:
        decision.block(f"sweep_too_old_for_stop({age})")
        return None

    buffer = max(cfg.sl_buffer_atr * a, price * cfg.sl_buffer_pct_min)
    sl = (extreme - buffer) if long_ else (extreme + buffer)

    # the zone edge only widens the stop when it is *close* - a far zone edge
    # would blow the risk out for no protective benefit
    edge = ctx.zone.low if long_ else ctx.zone.high
    if abs(edge - extreme) <= cfg.sl_zone_edge_atr * a:
        sl = min(sl, edge - buffer) if long_ else max(sl, edge + buffer)

    # ------------------------------------------------------------ entry zone
    gap = abs(price - level)
    pad = cfg.entry_pad_atr * a
    if gap <= 0.8 * a:
        lo, hi = min(price, level), max(price, level)
    elif long_:
        lo, hi = price - 0.35 * a, price + 0.10 * a
    else:
        lo, hi = price - 0.10 * a, price + 0.35 * a
    entry_low, entry_high = lo - pad, hi + pad
    if long_:
        entry_low = max(entry_low, sl + 0.15 * abs(price - sl))
    else:
        entry_high = min(entry_high, sl - 0.15 * abs(sl - price))
    if entry_high <= entry_low:
        entry_low, entry_high = price - pad, price + pad

    entry_ref = (entry_low + entry_high) / 2.0
    risk = abs(entry_ref - sl)
    if risk <= 0:
        return None

    risk_pct = risk / entry_ref
    if risk_pct < cfg.min_risk_pct:
        decision.block(f"risk_too_tight({risk_pct*100:.2f}%)")
        return None
    if risk_pct > cfg.max_risk_pct:
        decision.block(f"risk_too_wide({risk_pct*100:.2f}%)")
        return None

    # --------------------------------------------------------------- targets
    targets = decision.details.get("targets") or liquidity_targets(ctx, direction, entry_ref)
    if opposing_level:
        move = (opposing_level - entry_ref) if long_ else (entry_ref - opposing_level)
        if move > 0:
            targets = targets + [{"price": opposing_level, "strength": 0.6,
                                  "source": "zone", "label": "opposing zone",
                                  "distance_pct": move / entry_ref}]
            targets.sort(key=lambda t: t["distance_pct"])

    # How far price can plausibly travel from here.
    #
    # Measured against the 4H ATR, because that is the horizon a zone-to-zone
    # move plays out over. Without any cap a distant 4H zone gets offered as a
    # target however many sessions away it sits - which is how TP3 once landed
    # 119R out, at a price the market would have had to nearly triple to reach.
    #
    # The cap never falls below what a viable TP3 needs, so it only ever bites
    # on genuinely absurd targets rather than rejecting ordinary setups.
    htf_atr = float(ctx.zone.flags.get("htf_atr") or 0.0)
    if htf_atr <= 0:
        mid = ctx.candles.get(cfg.ltf_mid) or []
        htf_atr = (atr(mid, 14) * 4.0) if len(mid) >= 20 else a * 8.0
    reach = max(htf_atr * cfg.tp_reach_atr_mult, cfg.tp3_min_r * risk * 1.15)
    ceiling_r = min(cfg.tp3_max_r, reach / risk)

    sign = 1.0 if long_ else -1.0
    used: List[float] = []
    chosen: List[Optional[dict]] = []
    windows = ((cfg.tp1_min_r, min(cfg.tp1_max_r, ceiling_r), False),
               (cfg.tp2_min_r, min(cfg.tp2_max_r, ceiling_r), False),
               (cfg.tp3_min_r, ceiling_r, True))
    for min_r, max_r, by_strength in windows:
        pick = _pick_target(targets, entry_ref, risk, direction, min_r, max_r,
                            used, cfg.target_merge_pct, reach, by_strength)
        if pick:
            used.append(float(pick["price"]))
        chosen.append(pick)

    # Build the ladder in R space, then convert to prices. Doing it the other
    # way round - clamping prices and sorting them afterwards - let a capped
    # TP3 be shuffled into the TP1 slot at a reward below its own minimum.
    gap = cfg.tp_min_separation_r
    r1 = chosen[0]["r"] if chosen[0] else clamp(cfg.tp1_r, cfg.tp1_min_r,
                                               min(cfg.tp1_max_r, ceiling_r))
    r2 = chosen[1]["r"] if chosen[1] else clamp(cfg.tp2_r, cfg.tp2_min_r,
                                               min(cfg.tp2_max_r, ceiling_r))
    r3 = chosen[2]["r"] if chosen[2] else clamp(cfg.tp3_r, cfg.tp3_min_r, ceiling_r)
    r2 = max(r2, r1 + gap)
    r3 = max(r3, r2 + gap)
    if r3 > ceiling_r + 1e-9:
        r3 = ceiling_r
        r2 = min(r2, r3 - gap)
        r1 = min(r1, r2 - gap)
    if not (0 < r1 < r2 < r3) or r1 < cfg.tp1_min_r * 0.6:
        decision.block(f"no_reachable_target_ladder(ceiling {ceiling_r:.1f}R)")
        return None

    tp1 = entry_ref + sign * r1 * risk
    tp2 = entry_ref + sign * r2 * risk
    tp3 = entry_ref + sign * r3 * risk

    rr = abs(tp3 - entry_ref) / risk
    if rr < cfg.min_rr_after_cap:
        decision.block(f"rr_too_low({rr:.2f})")
        return None

    ok = (sl < entry_low < entry_high < tp1 < tp2 < tp3) if long_ \
        else (sl > entry_high > entry_low > tp1 > tp2 > tp3)
    if not ok:
        decision.block("level_ordering_invalid")
        return None

    target_meta = []
    for level, pick in zip((tp1, tp2, tp3), chosen):
        if pick and abs(float(pick["price"]) - level) / entry_ref < 1e-9:
            target_meta.append({"price": level, "source": pick["source"],
                                "label": pick.get("label", ""), "r": round(pick["r"], 2)})

    return Signal(
        symbol=ctx.symbol, direction=direction,
        entry_low=entry_low, entry_high=entry_high, entry_ref=entry_ref,
        sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
        grade=ctx.zone.grade, zone_score=ctx.zone.score,
        confidence=decision.confidence, reasons=list(decision.reasons),
        zone_low=ctx.zone.low, zone_high=ctx.zone.high,
        risk_pct=risk_pct, rr=rr, decimals=ctx.decimals, created_ts=now_ms(),
        meta={
            "exchange": ctx.exchange,
            "trend": decision.details.get("trend_state"),
            "counter_trend": decision.details.get("trend_conflict") in ("soft", "hard"),
            "reach_cap": reach,
            "sweep_level": level,
            "sweep_extreme": extreme,
            "sweep_age_bars": age,
            "sweep_tf": sweep.get("tf"),
            "sweep_structural": sweep.get("structural"),
            "zone_label": ctx.zone.flags.get("structural"),
            "zone_breakdown": ctx.zone.breakdown,
            "absorption": decision.details.get("absorption"),
            "execution": decision.details.get("execution"),
            "liquidity_bias": decision.details.get("liquidity_bias"),
            "heatmap": decision.details.get("heatmap"),
            "targets": target_meta,
            "vwap": decision.details.get("vwap"),
            "optional_confirms": decision.details.get("optional_confirms"),
            "atr": a,
        },
    )

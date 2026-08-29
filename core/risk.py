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
from .utils import get_logger, now_ms

log = get_logger("risk")


def _pick_target(targets: List[dict], entry: float, risk: float, direction: str,
                 min_r: float, used: List[float], merge_pct: float) -> Optional[dict]:
    """Nearest liquidity pool that clears `min_r` and is not already used."""
    for t in targets:
        level = float(t["price"])
        r = (level - entry) / risk if direction == "LONG" else (entry - level) / risk
        if r < min_r:
            continue
        if any(abs(level - u) / max(entry, 1e-9) < merge_pct for u in used):
            continue
        return {**t, "r": r}
    return None


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

    sign = 1.0 if long_ else -1.0
    used: List[float] = []
    chosen: List[Optional[dict]] = []
    for min_r in (cfg.tp1_min_r, cfg.tp2_min_r, cfg.tp3_min_r):
        pick = _pick_target(targets, entry_ref, risk, direction, min_r, used, cfg.target_merge_pct)
        if pick:
            used.append(float(pick["price"]))
        chosen.append(pick)

    tp1 = chosen[0]["price"] if chosen[0] else entry_ref + sign * cfg.tp1_r * risk
    tp2 = chosen[1]["price"] if chosen[1] else entry_ref + sign * cfg.tp2_r * risk
    tp3 = chosen[2]["price"] if chosen[2] else entry_ref + sign * cfg.tp3_r * risk

    # keep them ordered and separated even after substitutions
    ordered_tps = sorted([tp1, tp2, tp3], reverse=not long_)
    tp1, tp2, tp3 = ordered_tps
    if long_ and not (tp1 < tp2 < tp3):
        tp2 = max(tp2, tp1 + 0.35 * risk)
        tp3 = max(tp3, tp2 + 0.35 * risk)
    if not long_ and not (tp1 > tp2 > tp3):
        tp2 = min(tp2, tp1 - 0.35 * risk)
        tp3 = min(tp3, tp2 - 0.35 * risk)

    rr = abs(tp3 - entry_ref) / risk
    if rr < cfg.min_rr_after_cap:
        decision.block(f"rr_too_low({rr:.2f})")
        return None

    ok = (sl < entry_low < entry_high < tp1 < tp2 < tp3) if long_ \
        else (sl > entry_high > entry_low > tp1 > tp2 > tp3)
    if not ok:
        decision.block("level_ordering_invalid")
        return None

    target_meta = [{"price": c["price"], "source": c["source"], "label": c.get("label", ""),
                    "r": round(c["r"], 2)} for c in chosen if c]

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

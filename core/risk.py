"""
Turning a confirmed setup into actual levels.

The stop lives beyond the sweep extreme - the exact place the setup is wrong,
not an arbitrary percentage. Targets are R multiples, optionally capped at the
next opposing zone so we are not aiming through a wall of structure.
"""
from __future__ import annotations

from typing import Optional

from .indicators import atr
from .models import Decision, Signal
from .utils import get_logger, now_ms

log = get_logger("risk")


def build_signal(ctx, decision: Decision, cfg, opposing_level: Optional[float] = None) -> Optional[Signal]:
    direction = ctx.direction
    price = ctx.price
    sweep = decision.details.get("sweep") or {}
    fast = ctx.candles.get(cfg.ltf_fast) or []
    slow = ctx.candles.get(cfg.ltf_slow) or []
    a = atr(fast, 14) or atr(slow, 14)
    if a <= 0 or price <= 0:
        return None

    extreme = float(sweep.get("extreme") or (price - a))
    level = float(sweep.get("level") or price)

    buffer = max(cfg.sl_buffer_atr * a, price * cfg.sl_buffer_pct_min)
    if direction == "LONG":
        sl = min(extreme, ctx.zone.low) - buffer
    else:
        sl = max(extreme, ctx.zone.high) + buffer

    # ------------------------------------------------------------ entry zone
    gap = abs(price - level)
    pad = cfg.entry_pad_atr * a
    if gap <= 0.8 * a:
        lo, hi = min(price, level), max(price, level)
    elif direction == "LONG":
        lo, hi = price - 0.35 * a, price + 0.10 * a
    else:
        lo, hi = price - 0.10 * a, price + 0.35 * a
    entry_low, entry_high = lo - pad, hi + pad

    if direction == "LONG":
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

    sign = 1.0 if direction == "LONG" else -1.0
    tp1 = entry_ref + sign * cfg.tp1_r * risk
    tp2 = entry_ref + sign * cfg.tp2_r * risk
    tp3 = entry_ref + sign * cfg.tp3_r * risk

    # ------------------------------------------------- cap TP3 at structure
    if cfg.structural_tp_cap and opposing_level:
        pad3 = 0.20 * a
        if direction == "LONG" and opposing_level < tp3:
            tp3 = max(tp2 + 0.1 * risk, opposing_level - pad3)
        elif direction == "SHORT" and opposing_level > tp3:
            tp3 = min(tp2 - 0.1 * risk, opposing_level + pad3)

    rr = abs(tp3 - entry_ref) / risk
    if rr < cfg.min_rr_after_cap:
        decision.block(f"rr_too_low({rr:.2f})")
        return None

    ordered = (sl < entry_low < entry_high < tp1 < tp2 < tp3) if direction == "LONG" \
        else (sl > entry_high > entry_low > tp1 > tp2 > tp3)
    if not ordered:
        decision.block("level_ordering_invalid")
        return None

    return Signal(
        symbol=ctx.symbol,
        direction=direction,
        entry_low=entry_low,
        entry_high=entry_high,
        entry_ref=entry_ref,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        grade=ctx.zone.grade,
        zone_score=ctx.zone.score,
        confidence=decision.confidence,
        reasons=list(decision.reasons),
        zone_low=ctx.zone.low,
        zone_high=ctx.zone.high,
        risk_pct=risk_pct,
        rr=rr,
        decimals=ctx.decimals,
        created_ts=now_ms(),
        meta={
            "sweep_level": level,
            "sweep_extreme": extreme,
            "sweep_tf": sweep.get("tf"),
            "zone_breakdown": ctx.zone.breakdown,
            "absorption": decision.details.get("absorption"),
            "cvd": decision.details.get("cvd_divergence") or decision.details.get("cvd_reclaim"),
            "orderbook": {
                "stack_ratio": (decision.details.get("orderbook") or {}).get("stack_ratio"),
                "walls": ((decision.details.get("orderbook") or {}).get("walls") or {}).get("biggest"),
            },
            "optional_confirms": decision.details.get("optional_confirms"),
            "atr": a,
        },
    )

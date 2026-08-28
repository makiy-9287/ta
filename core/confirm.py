"""
The confirmation engine.

A high score on a zone gets a symbol *watched*, never traded. The trade is
only born when the full sequence prints:

    Zone -> Sweep -> Absorption -> Reclaim -> MSS

Mandatory gates (all must pass):
    * enough order flow to judge anything
    * liquidity sweep of a prior swing + reclaim of that level
    * absorption at the extreme (aggression that fails to move price)
    * CVD divergence or CVD reclaim
    * market structure shift on 3m or 5m
    * order book is not constantly pulling liquidity on approach
    * footprint is not a mess
    * setup is not fighting a strong 4H trend

Optional confirmations add confidence; a minimum count is still required.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .indicators import atr, cvd_divergence, cvd_reclaim, delta_zscore
from .models import Decision
from .structure import detect_mss, detect_sweep, reclaim_strength, structure_bias
from .utils import clamp, get_logger, mean

log = get_logger("confirm")

WEIGHTS = {
    "zone": 0.20,
    "sweep": 0.14,
    "absorption": 0.16,
    "cvd": 0.14,
    "mss": 0.12,
    "delta": 0.08,
    "imbalance": 0.06,
    "book": 0.10,
}


def _trend_conflict(direction: str, trend_state: str) -> bool:
    if direction == "LONG":
        return trend_state == "strong_down"
    return trend_state == "strong_up"


def evaluate(ctx, cfg, trend_state: str = "range") -> Decision:
    """Run every check for one armed symbol and return a Decision."""
    d = Decision(passed=False, direction=ctx.direction)
    book = ctx.book
    zone = ctx.zone
    direction = ctx.direction

    micro = ctx.candles.get(cfg.micro_interval) or []
    fast = ctx.candles.get(cfg.ltf_fast) or []
    slow = ctx.candles.get(cfg.ltf_slow) or []

    # ------------------------------------------------------------- data gates
    health = book.health(cfg.min_trades_for_flow)
    d.details["flow"] = health
    d.details["flow_age"] = round(ctx.flow_age)
    d.details["feed_age"] = round(ctx.feed_age)
    if not health["enough"]:
        d.block("thin_flow")
    if not ctx.flow_fresh(cfg.max_flow_age_sec):
        # never judge live order flow from a frozen feed - a silent socket
        # would otherwise keep re-scoring the same seed data indefinitely
        d.block(f"stale_flow({int(ctx.flow_age)}s)")
    if len(fast) < 30 or len(slow) < 30 or len(micro) < 40:
        d.block("insufficient_candles")
    if d.blockers:
        return d

    atr_fast = atr(fast, 14)
    atr_slow = atr(slow, 14)
    price = ctx.price
    d.details["price"] = price
    d.details["atr_fast"] = atr_fast

    # -------------------------------------------------------------- trend gate
    if cfg.respect_htf_trend and _trend_conflict(direction, trend_state):
        d.block("against_4h_trend")

    # --------------------------------------------------------- zone integrity
    if zone.touches >= cfg.max_zone_tests:
        d.block("zone_overtested")
    if not ctx.entry_window_ok(atr_fast):
        d.block("price_outside_entry_window")

    # ---------------------------------------------------- 1. liquidity sweep
    sweep = detect_sweep(slow, direction, cfg, zone=zone, atr_val=atr_slow)
    if not sweep.get("found"):
        sweep = detect_sweep(fast, direction, cfg, zone=zone, atr_val=atr_fast)
        sweep["tf"] = cfg.ltf_fast
    else:
        sweep["tf"] = cfg.ltf_slow
    d.details["sweep"] = sweep

    if not sweep.get("found"):
        d.block("no_liquidity_sweep")
    elif not sweep.get("reclaimed"):
        d.block("sweep_not_reclaimed")
    else:
        d.add(True, f"Liquidity sweep + reclaim ({sweep['tf']}, {sweep.get('pierce_atr')} ATR pierce)")

    # ------------------------------------------------------- 2. absorption
    absorb = book.absorption(direction, vol_mult=cfg.absorption_vol_mult,
                             recovery_frac=cfg.absorption_efficiency,
                             window_sec=cfg.footprint_window_min * 60)
    d.details["absorption"] = absorb
    if not absorb["found"]:
        d.block("no_absorption")
    else:
        d.add(True, f"Absorption at {absorb['level']:.{ctx.decimals}f} "
                    f"({absorb['ratio']}x avg level, {int(absorb['share']*100)}% aggressive)")

    # --------------------------------------------------- 3. CVD divergence
    div = cvd_divergence(micro, direction, lookback=90, pivot=2)
    rec = cvd_reclaim(micro, direction, lookback=45, recovery_frac=cfg.cvd_recovery_frac)
    d.details["cvd_divergence"] = div
    d.details["cvd_reclaim"] = rec
    if div.get("found"):
        d.add(True, f"CVD {div['type']} divergence (strength {div['strength']:.2f})")
    elif rec.get("found"):
        d.add(True, f"CVD reclaim ({int(rec['recovered']*100)}% of the excursion)")
    else:
        d.block("no_cvd_confirmation")

    # ------------------------------------------------------------- 4. MSS
    mss_fast = detect_mss(fast, direction, cfg, after_idx=None)
    mss_slow = detect_mss(slow, direction, cfg, after_idx=sweep.get("sweep_idx"))
    mss_ok = bool(mss_fast.get("found") or mss_slow.get("found"))
    d.details["mss"] = {"fast": mss_fast, "slow": mss_slow}
    if mss_ok:
        tf = cfg.ltf_fast if mss_fast.get("found") else cfg.ltf_slow
        d.add(True, f"Market structure shift on {tf}")
    else:
        d.block("no_mss")

    # ------------------------------------------------------- 5. order book
    ob = ctx.depth.analyse(direction, price, cfg)
    d.details["orderbook"] = ob
    if ob["liquidity_pulling"]:
        d.block("orderbook_liquidity_pulling")
    walls = ob["walls"]
    if walls.get("case_b"):
        d.add(True, "Resting liquidity executed and held (order book case B)")
    elif ob["supportive"]:
        d.add(True, f"Book stacked in our favour ({ob['stack_ratio']}x)")

    # ----------------------------------------------------- 6. footprint mess
    imb = book.imbalances(ratio=cfg.imbalance_ratio, window_sec=cfg.footprint_window_min * 60)
    d.details["imbalance"] = imb
    stack = imb["buy_stack"] if direction == "LONG" else imb["sell_stack"]
    if not imb["clean"]:
        d.block("messy_footprint")
    elif stack >= cfg.min_imbalance_stack:
        d.add(True, f"Stacked {'buy' if direction == 'LONG' else 'sell'} imbalances x{stack}")

    # -------------------------------------------------------- 7. delta extreme
    dex = book.delta_extreme(direction, z_threshold=cfg.delta_extreme_z,
                             window_sec=cfg.footprint_window_min * 60)
    kline_z = delta_zscore(micro, 60)
    d.details["delta_extreme"] = dex
    d.details["kline_delta_z"] = round(kline_z, 2)
    if dex["found"]:
        d.add(True, f"Delta extreme ({dex['z']} sigma {'sell' if direction == 'LONG' else 'buy'} pressure)")

    # ------------------------------------------------- 8. reclaim / follow-up
    strength = 0.0
    if sweep.get("found") and sweep.get("level"):
        base_atr = atr_slow if sweep.get("tf") == cfg.ltf_slow else atr_fast
        strength = reclaim_strength(slow if sweep.get("tf") == cfg.ltf_slow else fast,
                                    direction, float(sweep["level"]), base_atr)
        d.details["reclaim_atr"] = round(strength, 2)
        if strength >= 0.35:
            d.add(True, f"Holding above reclaim by {strength:.2f} ATR" if direction == "LONG"
                        else f"Holding below reclaim by {strength:.2f} ATR")

    bias = structure_bias(fast, 40)
    d.details["ltf_bias"] = bias
    if (direction == "LONG" and bias == "bullish") or (direction == "SHORT" and bias == "bearish"):
        d.add(True, f"LTF structure turned {bias}")

    # ------------------------------------------------------------ confidence
    conf = 0.0
    conf += WEIGHTS["zone"] * clamp((zone.score - 60) / 40.0, 0.0, 1.0)
    if sweep.get("found") and sweep.get("reclaimed"):
        disp = float(sweep.get("displacement_atr") or 0)
        conf += WEIGHTS["sweep"] * clamp(0.55 + disp / 3.0, 0.0, 1.0)
    if absorb["found"]:
        conf += WEIGHTS["absorption"] * clamp(absorb["ratio"] / (cfg.absorption_vol_mult * 2.2), 0.35, 1.0)
    if div.get("found"):
        conf += WEIGHTS["cvd"] * clamp(0.6 + float(div.get("strength") or 0), 0.0, 1.0)
    elif rec.get("found"):
        conf += WEIGHTS["cvd"] * 0.6
    if mss_ok:
        conf += WEIGHTS["mss"] * (1.0 if (mss_fast.get("found") and mss_slow.get("found")) else 0.75)
    if dex["found"]:
        conf += WEIGHTS["delta"] * clamp(abs(float(dex["z"])) / (cfg.delta_extreme_z * 2), 0.4, 1.0)
    if stack >= cfg.min_imbalance_stack:
        conf += WEIGHTS["imbalance"] * clamp(stack / 4.0, 0.4, 1.0)
    if walls.get("case_b"):
        conf += WEIGHTS["book"]
    elif ob["supportive"]:
        conf += WEIGHTS["book"] * 0.55

    d.confidence = round(clamp(conf, 0.0, 1.0), 3)

    # ------------------------------------------------------------- verdict
    optional = sum([
        1 if dex["found"] else 0,
        1 if stack >= cfg.min_imbalance_stack else 0,
        1 if (walls.get("case_b") or ob["supportive"]) else 0,
        1 if strength >= 0.35 else 0,
        1 if (direction == "LONG" and bias == "bullish") or (direction == "SHORT" and bias == "bearish") else 0,
    ])
    d.details["optional_confirms"] = optional

    if optional < cfg.min_optional_confirms:
        d.block("insufficient_secondary_confirmations")
    if d.confidence < cfg.min_confidence:
        d.block(f"confidence_below_threshold({d.confidence:.2f})")

    d.passed = not d.blockers
    return d

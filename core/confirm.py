"""
The confirmation engine.

A high zone score gets a symbol *watched*. The trade is only born when the
full institutional sequence prints, in order:

    where does the money rest?  ->  is price being driven into liquidity?
    ->  was that liquidity actually taken (recent sweep)?
    ->  did someone absorb the aggression at the extreme?
    ->  is a real participant working there (iceberg / TWAP)?
    ->  did price reclaim and shift structure?

The ordering matters. A spike in delta, absorption or CVD proves nothing by
itself - momentum like that evaporates in seconds. It is only meaningful when
it happens at a level where liquidity was resting, in the direction of the
larger pool that price still has to travel to. So liquidity context is checked
*first*, and everything else is treated as evidence about that context rather
than as a signal in its own right.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .indicators import atr, cvd_divergence, cvd_reclaim, delta_zscore, htf_trend
from .models import Decision
from .structure import detect_mss, detect_sweep, reclaim_strength, structure_bias
from .utils import clamp, get_logger

log = get_logger("confirm")

WEIGHTS = {
    "zone": 0.14,
    "liquidity": 0.16,      # where the money rests, and the sweep that took it
    "sweep": 0.12,
    "absorption": 0.14,
    "institutional": 0.12,  # iceberg / TWAP working the level
    "cvd": 0.10,
    "mss": 0.10,
    "book": 0.06,
    "delta": 0.04,
    "imbalance": 0.02,
}


def _trend_conflict(direction: str, trend_state: str) -> str:
    """'none' | 'soft' | 'hard'."""
    if direction == "LONG":
        if trend_state == "strong_down":
            return "hard"
        return "soft" if trend_state == "down" else "none"
    if trend_state == "strong_up":
        return "hard"
    return "soft" if trend_state == "up" else "none"


def evaluate(ctx, cfg, trend_state: str = "range") -> Decision:
    d = Decision(passed=False, direction=ctx.direction)
    book = ctx.book
    zone = ctx.zone
    direction = ctx.direction
    long_ = direction == "LONG"

    ctx.refresh_derived()

    micro = ctx.candles.get(cfg.micro_interval) or []
    fast = ctx.candles.get(cfg.ltf_fast) or []
    slow = ctx.candles.get(cfg.ltf_slow) or []
    mid = ctx.candles.get(cfg.ltf_mid) or []

    # ------------------------------------------------------------- data gates
    health = book.health(cfg.min_trades_for_flow)
    d.details["flow"] = health
    d.details["flow_age"] = round(ctx.flow_age)
    d.details["exchange"] = ctx.exchange
    if not health["enough"]:
        d.block("thin_flow")
    if not ctx.flow_fresh(cfg.max_flow_age_sec):
        d.block(f"stale_flow({int(ctx.flow_age)}s)")
    if len(fast) < 30 or len(slow) < 30 or len(micro) < 40:
        d.block("insufficient_candles")
    if d.blockers:
        return d

    atr_fast = atr(fast, 14)
    atr_slow = atr(slow, 14)
    price = ctx.price
    d.details["price"] = price

    if not ctx.entry_window_ok(atr_fast):
        d.block("price_outside_entry_window")
    if zone.touches >= cfg.max_zone_tests:
        d.block("zone_overtested")

    # ===================================================== 1. LIQUIDITY CONTEXT
    # Before anything else: where is the money, and is this trade heading
    # toward it or away from it?
    liq = ctx.ltf_liquidity
    htf_liq = ctx.htf_liquidity
    heatmap = ctx.heatmap

    targets = liquidity_targets(ctx, direction, price)
    d.details["targets"] = [t for t in targets[:4]]
    bias = liq.resting_bias(price, cfg.liquidity_span_pct) if liq else {}
    htf_bias = htf_liq.resting_bias(price, cfg.liquidity_span_pct * 2) if htf_liq else {}
    d.details["liquidity_bias"] = bias
    d.details["htf_liquidity_bias"] = htf_bias
    d.details["heatmap"] = heatmap.summary(price)

    if not targets:
        # nothing above (or below) for price to travel to - no destination,
        # no trade, however clean the order flow looks
        d.block("no_liquidity_target")
    else:
        best = targets[0]
        d.add(True, f"Liquidity target {best['price']:.{ctx.decimals}f} "
                    f"({best['source']}, {int(best['strength']*100)}%)")

    ratio = float(bias.get("ratio") or 1.0)
    against = (ratio < cfg.liquidity_bias_min) if long_ else (ratio > 1.0 / cfg.liquidity_bias_min)
    support_pool = heatmap.support_pool(direction, price, cfg.heatmap_support_pct)
    if against and support_pool is None:
        d.block("liquidity_rests_against_trade")
    elif support_pool is not None and support_pool.verdict in ("consumed", "iceberg", "resting"):
        d.add(True, f"Heatmap {support_pool.verdict} pool at "
                    f"{support_pool.price:.{ctx.decimals}f} holding the level")

    # ============================================================ 2. THE SWEEP
    sweep = detect_sweep(slow, direction, cfg, zone=zone, atr_val=atr_slow, liquidity=liq)
    sweep_tf = cfg.ltf_slow
    if not sweep.get("found"):
        sweep = detect_sweep(fast, direction, cfg, zone=zone, atr_val=atr_fast, liquidity=liq)
        sweep_tf = cfg.ltf_fast
    sweep["tf"] = sweep_tf
    d.details["sweep"] = sweep

    if not sweep.get("found"):
        d.block("no_liquidity_sweep")
    else:
        if not sweep.get("reclaimed"):
            d.block("sweep_not_reclaimed")
        age = int(sweep.get("age_bars") or 99)
        if age > cfg.sweep_max_age_bars:
            d.block(f"sweep_stale({age}_bars)")
        elif not sweep.get("fresh"):
            # inside the tolerance but not in the freshest window: allowed, but
            # it has to earn it elsewhere
            d.details["sweep_aging"] = age
        if cfg.require_structural_sweep and not sweep.get("structural"):
            d.block("sweep_took_no_structural_liquidity")
        if not d.blockers or "no_liquidity_sweep" not in d.blockers:
            label = (sweep.get("structural") or {}).get("label", "level")
            d.add(True, f"Swept {label} at {sweep.get('level', 0):.{ctx.decimals}f} "
                        f"{age} bars ago, reclaimed")

    # ========================================================= 3. ABSORPTION
    absorb = book.absorption(direction, vol_mult=cfg.absorption_vol_mult,
                             recovery_frac=cfg.absorption_efficiency,
                             window_sec=cfg.footprint_window_min * 60)
    d.details["absorption"] = absorb
    if not absorb["found"]:
        d.block("no_absorption")
    else:
        d.add(True, f"Absorption at {absorb['level']:.{ctx.decimals}f} "
                    f"({absorb['ratio']}x avg level, {int(absorb['share']*100)}% aggressive)")

    # =============================================== 4. INSTITUTIONAL FOOTPRINT
    execution = ctx.execution or {}
    iceberg = execution.get("iceberg") or {}
    slicing = execution.get("slicing") or {}
    d.details["execution"] = execution
    institutional = bool(execution.get("institutional"))

    if execution.get("supportive_iceberg"):
        d.add(True, f"Iceberg refilling {iceberg.get('side')} at "
                    f"{iceberg.get('price', 0):.{ctx.decimals}f} "
                    f"({iceberg.get('ratio')}x displayed size)")
    elif slicing.get("twap"):
        d.add(True, f"TWAP slicing detected ({slicing.get('count')} clips of "
                    f"{slicing.get('clip')}, regularity {slicing.get('regularity')})")
    if cfg.require_institutional and not institutional:
        d.block("no_institutional_participation")

    # =============================================================== 5. CVD
    cvd_candles = ctx.micro_cvd_candles()
    div = cvd_divergence(cvd_candles, direction, lookback=90, pivot=2)
    rec = cvd_reclaim(cvd_candles, direction, lookback=45, recovery_frac=cfg.cvd_recovery_frac)
    d.details["cvd_divergence"] = div
    d.details["cvd_reclaim"] = rec
    if div.get("found"):
        d.add(True, f"CVD {div['type']} divergence (strength {div['strength']:.2f})")
    elif rec.get("found"):
        d.add(True, f"CVD reclaim ({int(rec['recovered']*100)}% of the excursion)")
    else:
        d.block("no_cvd_confirmation")

    # =============================================================== 6. MSS
    mss_fast = detect_mss(fast, direction, cfg)
    mss_slow = detect_mss(slow, direction, cfg, after_idx=sweep.get("sweep_idx"))
    mss_ok = bool(mss_fast.get("found") or mss_slow.get("found"))
    d.details["mss"] = {"fast": mss_fast, "slow": mss_slow}
    if mss_ok:
        d.add(True, f"Market structure shift on "
                    f"{cfg.ltf_fast if mss_fast.get('found') else cfg.ltf_slow}")
    else:
        d.block("no_mss")

    # ================================================ 7. MULTI-TIMEFRAME CHECK
    mid_bias = structure_bias(mid, 40) if len(mid) >= 20 else "neutral"
    fast_bias = structure_bias(fast, 40)
    d.details["bias"] = {"mid": mid_bias, "fast": fast_bias, "htf": trend_state}
    want = "bullish" if long_ else "bearish"
    opposite = "bearish" if long_ else "bullish"
    mtf_score = sum([mid_bias == want, fast_bias == want])
    if mid_bias == opposite and fast_bias == opposite:
        d.block("all_lower_timeframes_opposed")
    if mtf_score:
        d.add(True, f"{cfg.ltf_mid}/{cfg.ltf_fast} structure aligned ({mtf_score}/2)")

    # ============================================== 8. TREND / COUNTER-TREND
    conflict = _trend_conflict(direction, trend_state)
    d.details["trend_conflict"] = conflict
    if cfg.respect_htf_trend and conflict == "hard":
        d.block("against_4h_trend")
    elif conflict == "soft" and cfg.counter_trend_policy != "allow":
        # trading back into a trend needs a materially better setup: a top
        # grade zone, a fresh structural sweep and a real participant
        strict_ok = (zone.grade == "A+" and bool(sweep.get("fresh"))
                     and bool(sweep.get("structural")) and institutional)
        if cfg.counter_trend_policy == "block":
            d.block("counter_trend_blocked")
        elif not strict_ok:
            d.block("counter_trend_needs_A+_fresh_sweep_and_institutional_flow")
        else:
            d.add(True, "Counter-trend setup meets the strict criteria")

    # ============================================================ 9. BOOK
    ob = ctx.depth.analyse(direction, price, cfg)
    d.details["orderbook"] = ob
    walls = ob["walls"]
    if ob["liquidity_pulling"]:
        d.block("orderbook_liquidity_pulling")
    if walls.get("case_b"):
        d.add(True, "Resting liquidity executed and held (order book case B)")
    elif ob["supportive"]:
        d.add(True, f"Book stacked in our favour ({ob['stack_ratio']}x)")

    # ====================================================== 10. FOOTPRINT / DELTA
    imb = book.imbalances(ratio=cfg.imbalance_ratio, window_sec=cfg.footprint_window_min * 60)
    d.details["imbalance"] = imb
    stack = imb["buy_stack"] if long_ else imb["sell_stack"]
    if not imb["clean"]:
        d.block("messy_footprint")
    elif stack >= cfg.min_imbalance_stack:
        d.add(True, f"Stacked {'buy' if long_ else 'sell'} imbalances x{stack}")

    dex = book.delta_extreme(direction, z_threshold=cfg.delta_extreme_z,
                             window_sec=cfg.footprint_window_min * 60)
    d.details["delta_extreme"] = dex
    if dex["found"]:
        d.add(True, f"Delta extreme ({dex['z']} sigma "
                    f"{'sell' if long_ else 'buy'} pressure absorbed)")

    # ================================================== 11. RECLAIM / VWAP
    strength = 0.0
    if sweep.get("found") and sweep.get("level"):
        base = slow if sweep_tf == cfg.ltf_slow else fast
        strength = reclaim_strength(base, direction, float(sweep["level"]),
                                    atr_slow if sweep_tf == cfg.ltf_slow else atr_fast)
        d.details["reclaim_atr"] = round(strength, 2)
        if strength >= 0.35:
            d.add(True, f"Holding the reclaim by {strength:.2f} ATR")

    vwap = ctx.vwap or {}
    d.details["vwap"] = vwap
    if vwap.get("available") and vwap.get("favourable"):
        d.add(True, f"Price on the right side of VWAP (z={vwap.get('z')})")

    # ========================================================== CONFIDENCE
    conf = 0.0
    conf += WEIGHTS["zone"] * clamp((zone.score - 60) / 40.0, 0.0, 1.0)
    if targets:
        best = targets[0]
        conf += WEIGHTS["liquidity"] * clamp(0.45 + best["strength"] * 0.55, 0.0, 1.0)
    if sweep.get("found") and sweep.get("reclaimed"):
        disp = float(sweep.get("displacement_atr") or 0)
        fresh_bonus = 1.0 if sweep.get("fresh") else 0.7
        conf += WEIGHTS["sweep"] * clamp(0.5 + disp / 3.0, 0.0, 1.0) * fresh_bonus
    if absorb["found"]:
        conf += WEIGHTS["absorption"] * clamp(absorb["ratio"] / (cfg.absorption_vol_mult * 2.2), 0.35, 1.0)
    if execution.get("supportive_iceberg"):
        conf += WEIGHTS["institutional"]
    elif slicing.get("twap"):
        conf += WEIGHTS["institutional"] * 0.6
    if div.get("found"):
        conf += WEIGHTS["cvd"] * clamp(0.6 + float(div.get("strength") or 0), 0.0, 1.0)
    elif rec.get("found"):
        conf += WEIGHTS["cvd"] * 0.6
    if mss_ok:
        conf += WEIGHTS["mss"] * (1.0 if (mss_fast.get("found") and mss_slow.get("found")) else 0.75)
    if walls.get("case_b"):
        conf += WEIGHTS["book"]
    elif ob["supportive"]:
        conf += WEIGHTS["book"] * 0.55
    if dex["found"]:
        conf += WEIGHTS["delta"] * clamp(abs(float(dex["z"])) / (cfg.delta_extreme_z * 2), 0.4, 1.0)
    if stack >= cfg.min_imbalance_stack:
        conf += WEIGHTS["imbalance"]
    if conflict == "soft":
        conf -= cfg.counter_trend_penalty

    d.confidence = round(clamp(conf, 0.0, 1.0), 3)

    optional = sum([
        1 if dex["found"] else 0,
        1 if stack >= cfg.min_imbalance_stack else 0,
        1 if (walls.get("case_b") or ob["supportive"]) else 0,
        1 if strength >= 0.35 else 0,
        1 if mtf_score else 0,
        1 if institutional else 0,
        1 if (vwap.get("available") and vwap.get("favourable")) else 0,
        1 if support_pool is not None else 0,
    ])
    d.details["optional_confirms"] = optional

    if optional < cfg.min_optional_confirms:
        d.block("insufficient_secondary_confirmations")
    threshold = cfg.min_confidence + (cfg.counter_trend_penalty if conflict == "soft" else 0.0)
    if d.confidence < threshold:
        d.block(f"confidence_below_threshold({d.confidence:.2f}<{threshold:.2f})")

    d.passed = not d.blockers
    return d


def liquidity_targets(ctx, direction: str, price: float) -> List[Dict[str, object]]:
    """
    Everywhere price could reasonably be drawn to, ranked by distance.

    Three sources, because they see different things: the heatmap sees resting
    size that has actually persisted, the structural map sees stop clusters at
    swing points, and the zone map sees higher-timeframe supply/demand.
    """
    cfg = ctx.cfg
    out: List[Dict[str, object]] = []
    seen: List[float] = []

    def push(level: float, strength: float, source: str, label: str = "") -> None:
        if level <= 0:
            return
        move = (level - price) if direction == "LONG" else (price - level)
        if move <= 0:
            return
        if move / price < cfg.target_min_distance_pct:
            return
        for s in seen:
            if abs(s - level) / price < cfg.target_merge_pct:
                return
        seen.append(level)
        out.append({"price": level, "strength": round(clamp(strength, 0.0, 1.0), 3),
                    "source": source, "label": label,
                    "distance_pct": round(move / price, 5)})

    for pool in ctx.heatmap.target_pools(direction, price, cfg.heatmap_target_strength):
        push(pool.price, pool.strength, "heatmap", pool.verdict)

    for src, tag in ((ctx.ltf_liquidity, "structure"), (ctx.htf_liquidity, "htf-structure")):
        if src is None:
            continue
        levels = src.pools_above(price) if direction == "LONG" else src.pools_below(price)
        for lvl in levels[:6]:
            # untapped pools pull hardest; a level already swept is weaker
            push(lvl.price, lvl.strength * (1.0 if not lvl.swept else 0.55), tag, lvl.label)

    out.sort(key=lambda t: t["distance_pct"])
    return out

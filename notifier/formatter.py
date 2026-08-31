"""Message templates. Telegram HTML parse mode."""
from __future__ import annotations

import json
import time
from typing import Dict, List, Optional

from core.models import Signal
from core.utils import esc, fmt_price, fmt_pct, fmt_usd, human_delta, ts_to_str

ARROW = {"LONG": "🟢", "SHORT": "🔴"}
STATUS_ICON = {
    "WIN": "🏆", "LOSS": "🛑", "BREAKEVEN": "⚖️", "EXPIRED": "⌛",
    "TP1": "✅", "TP2": "✅", "TP3": "🏆", "CANCELLED": "🚫",
}


def _p(v: float, d: int) -> str:
    return fmt_price(v, d)


# --------------------------------------------------------------------- signal
def signal_message(sig: Signal, signal_id: int, zone_grade_note: str = "") -> str:
    d = sig.decimals
    risk = abs(sig.entry_ref - sig.sl)
    meta = sig.meta or {}
    targets = {round(float(t["price"]), 10): t for t in (meta.get("targets") or [])}

    def target_note(level: float) -> str:
        t = targets.get(round(level, 10))
        if not t:
            return ""
        label = t.get("label") or t.get("source")
        return f"  <i>{esc(str(label))}</i>"

    lines = [
        f"🎯 <b>SNIPER SIGNAL</b> · {sig.grade} ({sig.zone_score}/100)",
        f"{ARROW[sig.direction]} <b>{sig.direction}</b> · <b>{esc(sig.symbol)}</b>"
        f"  <code>{esc(str(meta.get('exchange', '')))}</code>",
        "",
        f"<b>Entry zone</b>  <code>{_p(sig.entry_low, d)} – {_p(sig.entry_high, d)}</code>",
        f"<b>Stop loss</b>   <code>{_p(sig.sl, d)}</code>  ({fmt_pct(-sig.risk_pct)})",
    ]
    age = meta.get("sweep_age_bars")
    if age is not None:
        lines.append(f"<i>stop sits beyond the sweep wick from {age} bars ago</i>")
    lines += [
        "",
        f"<b>TP1</b>  <code>{_p(sig.tp1, d)}</code>  ({(abs(sig.tp1 - sig.entry_ref)/risk):.1f}R){target_note(sig.tp1)}",
        f"<b>TP2</b>  <code>{_p(sig.tp2, d)}</code>  ({(abs(sig.tp2 - sig.entry_ref)/risk):.1f}R){target_note(sig.tp2)}",
        f"<b>TP3</b>  <code>{_p(sig.tp3, d)}</code>  ({(abs(sig.tp3 - sig.entry_ref)/risk):.1f}R){target_note(sig.tp3)}",
        "",
        "<b>Order flow confluence</b>",
    ]
    for r in sig.reasons[:9]:
        lines.append(f"✔️ {esc(r)}")

    # institutional participation
    ex = meta.get("execution") or {}
    ice = ex.get("iceberg") or {}
    slic = ex.get("slicing") or {}
    inst = []
    if ex.get("supportive_iceberg"):
        inst.append(f"iceberg {ice.get('ratio')}x displayed at {_p(float(ice.get('price') or 0), d)}")
    if slic.get("twap"):
        inst.append(f"TWAP {slic.get('count')}x{slic.get('clip')} clips")
    if inst:
        lines += ["", f"🏛 <b>Institutional</b>: {esc(', '.join(inst))}"]

    bias = meta.get("liquidity_bias") or {}
    heat = meta.get("heatmap") or {}
    if bias.get("bias"):
        lines.append(f"💧 <b>Liquidity</b> resting {esc(str(bias['bias']))} "
                     f"(ratio {bias.get('ratio')}) · heatmap imbalance {heat.get('imbalance')}")

    kind = "demand" if sig.direction == "LONG" else "supply"
    label = meta.get("zone_label")
    label_txt = f" [{esc(str(label))}]" if label else ""
    lines += [
        f"🧭 <b>Zone</b> 4H {kind}{label_txt} "
        f"<code>{_p(sig.zone_low, d)}–{_p(sig.zone_high, d)}</code>{zone_grade_note}",
        f"<b>Confidence</b> {int(sig.confidence*100)}% · <b>R:R</b> {sig.rr:.1f} · "
        f"<b>Risk</b> {sig.risk_pct*100:.2f}%",
        f"<code>#{signal_id} · {ts_to_str(sig.created_ts)} UTC</code>",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------- trade alerts
def tp_alert(trade: dict, level: str, info: dict) -> str:
    d = trade.get("decimals", 4)
    price = info.get("price", 0.0)
    new_stop = info.get("new_stop")
    lines = [
        f"{STATUS_ICON.get(level, '✅')} <b>{level} HIT</b> · {esc(trade['symbol'])} {trade['direction']} <code>#{trade['id']}</code>",
        f"Price <code>{_p(price, d)}</code>  ({info.get('r', 0):+.2f}R)",
    ]
    if new_stop:
        note = "breakeven" if level == "TP1" else "TP1"
        lines.append(f"🔒 Stop moved to {note}: <code>{_p(new_stop, d)}</code>")
    if level != "TP3":
        lines.append("Setup still running.")
    return "\n".join(lines)


def close_alert(trade: dict, status: str, info: dict) -> str:
    d = trade.get("decimals", 4)
    price = info.get("price", 0.0)
    r = info.get("r", 0.0)
    pct = info.get("pct", 0.0)
    head = {
        "WIN": "🏆 <b>SETUP COMPLETE</b>",
        "LOSS": "🛑 <b>STOP LOSS</b>",
        "BREAKEVEN": "⚖️ <b>CLOSED AT BREAKEVEN</b>",
        "EXPIRED": "⌛ <b>SETUP EXPIRED</b>",
        "CANCELLED": "🚫 <b>SETUP CANCELLED</b>",
    }.get(status, f"<b>{status}</b>")
    age = human_delta((time.time() * 1000 - (trade.get("created_ts") or 0)) / 1000)
    return "\n".join([
        f"{head} · {esc(trade['symbol'])} {trade['direction']} <code>#{trade['id']}</code>",
        f"Exit <code>{_p(price, d)}</code> · <b>{r:+.2f}R</b> ({fmt_pct(pct)})",
        f"Duration {age} · {esc(info.get('reason', ''))}",
        f"↩️ {esc(trade['symbol'])} back in the watchlist pool.",
    ])


# ------------------------------------------------------------------- reports
def status_message(engine: dict, trades: List[dict]) -> str:
    lines = [
        "📊 <b>ENGINE STATUS</b>",
        f"Uptime <b>{engine['uptime']}</b> · "
        + ("😴 asleep until " + esc(engine.get("next_wake", "")) if engine.get("asleep")
           else ("⏸ paused" if engine["paused"] else "🟢 hunting")),
        f"Watchlist <b>{engine['watchlist']}</b> · Zones <b>{engine['zones']}</b> "
        f"(A+ {engine['a_plus']}) · Armed <b>{engine['armed']}</b>",
        "Feeds: " + " · ".join(f"{esc(k)} {v}" for k, v in
                               (engine.get("exchanges") or {}).items()),
        f"Signals today <b>{engine['signals_today']}</b> · Open <b>{len(trades)}</b>",
    ]
    if trades:
        lines.append("")
        lines.append("<b>Open setups</b>")
        for t in trades:
            d = t.get("decimals", 4)
            lines.append(
                f"{ARROW[t['direction']]} <code>#{t['id']}</code> {esc(t['symbol'])} "
                f"{t['status']} · {t['r_now']:+.2f}R · {_p(t['price'], d)}")
    else:
        lines.append("\nNo open setups. Waiting for price to reach a graded zone.")
    return "\n".join(lines)


def active_message(trades: List[dict]) -> str:
    if not trades:
        return "No active setups right now."
    out = ["🎯 <b>ACTIVE SETUPS</b>"]
    for t in trades:
        d = t.get("decimals", 4)
        stop = t.get("sl_current") or t["sl"]
        out += [
            "",
            f"{ARROW[t['direction']]} <b>{esc(t['symbol'])}</b> {t['direction']} <code>#{t['id']}</code> · {t['status']}",
            f"Entry <code>{_p(t['entry_ref'], d)}</code> · Now <code>{_p(t['price'], d)}</code> "
            f"(<b>{t['r_now']:+.2f}R</b>)",
            f"Stop <code>{_p(stop, d)}</code> · TP1 <code>{_p(t['tp1'], d)}</code> · "
            f"TP2 <code>{_p(t['tp2'], d)}</code> · TP3 <code>{_p(t['tp3'], d)}</code>",
            f"MFE {t.get('mfe_r') or 0:+.2f}R · MAE {t.get('mae_r') or 0:+.2f}R · age {t['age_h']:.1f}h",
        ]
    return "\n".join(out)


def watchlist_message(rows: List[dict], threshold: float, next_refresh: str) -> str:
    if not rows:
        return "Watchlist is empty."
    top = rows[:25]
    lines = [
        f"👁 <b>WATCHLIST</b> · {len(rows)} symbols above {fmt_usd(threshold)} 24h volume",
        f"Next rebuild in {next_refresh}",
        "",
    ]
    for i, r in enumerate(top, 1):
        lines.append(f"{i:>2}. {esc(r['symbol']):<14} {fmt_usd(r['quote_volume'])}")
    if len(rows) > len(top):
        lines.append(f"… and {len(rows) - len(top)} more")
    return "\n".join(lines)


def zones_message(symbol: str, rows: List[dict], price: Optional[float] = None) -> str:
    if not rows:
        return f"No A/A+ zones stored for {esc(symbol)} right now."
    lines = [f"🧭 <b>{esc(symbol)} ZONES</b>"]
    if price:
        lines.append(f"CMP <code>{price:g}</code>")
    for z in rows:
        try:
            meta = json.loads(z.get("meta") or "{}")
        except ValueError:
            meta = {}
        bd = meta.get("breakdown", {})
        flags = meta.get("flags", {})
        icon = "🟩" if z["kind"] == "demand" else "🟥"
        fresh = "fresh" if z["touches"] == 0 else f"tested {z['touches']}x"
        lines += [
            "",
            f"{icon} <b>{z['grade']} {z['score']}/100</b> · {z['kind']} {z['tf']}",
            f"<code>{z['low']:g} – {z['high']:g}</code> · {fresh}",
            f"HTF {bd.get('htf_confluence', 0)} · React {bd.get('reaction', 0)} · "
            f"Liq {bd.get('liquidity', 0)} · Flow {bd.get('flow_history', 0)} · Fresh {bd.get('freshness', 0)}",
        ]
        extras = [k for k in ("mtf_overlap", "sweep_potential", "absorption", "cvd_div") if flags.get(k)]
        if extras:
            lines.append("· " + ", ".join(extras))
    return "\n".join(lines)


def signals_message(rows: List[dict]) -> str:
    if not rows:
        return "No signals recorded yet."
    lines = ["🗂 <b>RECENT SIGNALS</b>"]
    for r in rows:
        d = r.get("decimals", 4)
        icon = STATUS_ICON.get(r["status"], "🔵")
        res = f"{r['result_r']:+.2f}R" if r.get("result_r") is not None else r["status"]
        lines.append(
            f"{icon} <code>#{r['id']}</code> {esc(r['symbol'])} {r['direction']} "
            f"{_p(r['entry_ref'], d)} → {res} · {ts_to_str(r['created_ts'])}")
    return "\n".join(lines)


def pnl_message(perf: dict, period: str, breakdown: List[dict]) -> str:
    if not perf["closed"]:
        return f"No closed setups for <b>{period}</b> yet."
    lines = [
        f"💰 <b>PERFORMANCE · {period.upper()}</b>",
        "",
        f"Closed <b>{perf['closed']}</b> · Wins <b>{perf['wins']}</b> · "
        f"Losses <b>{perf['losses']}</b> · BE <b>{perf['flat']}</b>",
        f"Win rate <b>{perf['winrate']:.1f}%</b>",
        f"Total <b>{perf['total_r']:+.2f}R</b> · Average <b>{perf['avg_r']:+.2f}R</b>",
        f"TP1 {perf['tp1_hits']} · TP2 {perf['tp2_hits']} · TP3 {perf['tp3_hits']}",
    ]
    if perf["best"]:
        b = perf["best"]
        lines.append(f"Best <code>#{b['id']}</code> {esc(b['symbol'])} {b['result_r']:+.2f}R")
    if perf["worst"]:
        w = perf["worst"]
        lines.append(f"Worst <code>#{w['id']}</code> {esc(w['symbol'])} {w['result_r']:+.2f}R")
    if breakdown:
        lines += ["", "<b>By symbol</b>"]
        for row in breakdown:
            lines.append(f"{esc(row['symbol']):<12} {row['n']}x · {(row['r'] or 0):+.2f}R "
                         f"({row['wins']}W)")
    lines.append("\n<i>R = multiples of the risk defined by the signal's stop. "
                 "No position sizing is assumed.</i>")
    return "\n".join(lines)


def report_message(perf: dict, period: str, breakdown: List[dict], engine: dict) -> str:
    head = pnl_message(perf, period, breakdown)
    grades: Dict[str, List[float]] = {}
    for r in perf["rows"]:
        grades.setdefault(r.get("grade") or "?", []).append(r.get("result_r") or 0.0)
    contexts: Dict[str, List[float]] = {}
    for r in perf["rows"]:
        try:
            meta = json.loads(r.get("meta") or "{}")
        except ValueError:
            meta = {}
        key = "counter-trend" if meta.get("counter_trend") else "with-trend"
        contexts.setdefault(key, []).append(r.get("result_r") or 0.0)

    extra = []
    if len(contexts) > 1 or contexts:
        extra += ["", "<b>By trend context</b>"]
        for name, vals in sorted(contexts.items()):
            wins = len([v for v in vals if v > 0])
            extra.append(f"{name:<14} {len(vals)}x · {sum(vals):+.2f}R · {wins}/{len(vals)} won")

    extra += ["", "<b>By zone grade</b>"]
    for g, vals in sorted(grades.items()):
        wins = len([v for v in vals if v > 0])
        extra.append(f"{g:<3} {len(vals)}x · {sum(vals):+.2f}R · {wins}/{len(vals)} won")
    extra += ["", f"<i>Engine uptime {engine['uptime']}, "
                  f"{engine['signals_total']} signals generated in total.</i>"]
    return head + "\n" + "\n".join(extra)


def stats_message(engine: dict, armed: List[dict]) -> str:
    lines = [
        "⚙️ <b>ENGINE INTERNALS</b>",
        f"Scan cycles <b>{engine['cycles']}</b> · Evaluations <b>{engine['evaluations']}</b>",
        f"Zones built <b>{engine['zones']}</b> (A+ {engine['a_plus']}, A {engine['a_grade']})",
        f"Armed now <b>{len(armed)}</b> / {engine['max_armed']} · Rejected setups <b>{engine['rejected']}</b>",
    ]
    if engine.get("top_blockers"):
        lines += ["", "<b>Most common rejections</b>"]
        for tag, n in engine["top_blockers"]:
            lines.append(f"· {esc(tag)} — {n}")
    if armed:
        lines += ["", "<b>Currently armed</b>"]
        for a in armed:
            venue = f" [{esc(a.get('exchange', ''))}]"
            inst = " 🏛" if a.get("institutional") else ""
            feed = f" · feed {a['feed_age']}s" if a.get("feed_age", 0) > 60 else ""
            polls = f" · {a['rest_polls']} REST polls" if a.get("rest_polls") else ""
            lines.append(
                f"{esc(a['symbol'])}{venue}{inst} {a['direction']} · zone {a['score']} · "
                f"{a['trades']} trades · {a['age_min']}m{feed}{polls}")
            if a["blockers"]:
                lines.append("   waiting on: " + esc(", ".join(a["blockers"])))
    return "\n".join(lines)


def health_message(h: dict) -> str:
    lines = [
        "🩺 <b>SYSTEM HEALTH</b>",
        f"Uptime <b>{h['uptime']}</b> · Memory <b>{h['rss_mb']:.0f} MB</b>",
    ]
    for venue, v in (h.get("venues") or {}).items():
        pen = f" · ⚠️ penalty {v['penalty']}s" if v.get("penalty") else ""
        lines.append(f"· <b>{esc(venue)}</b> budget {v['used']}/{v['budget']}{pen}")

    assign = h.get("assignment") or {}
    if assign:
        lines.append("Stream split: " + " · ".join(
            f"<b>{esc(k)}</b> {v}" for k, v in assign.items()))
    trades = h.get("venue_trades") or {}
    if trades:
        lines.append("Trades received: " + " · ".join(
            f"{esc(k)} <b>{v:,}</b>" for k, v in trades.items()))
    blocked = h.get("venue_blocked") or []
    if blocked:
        lines.append(f"⚠️ <b>{esc(', '.join(blocked))}</b> feed delivered no trades — "
                     f"streaming moved to the working venue (REST history unaffected)")

    feed = h.get("feed") or {}
    lines += [
        f"Feed queue <b>{feed.get('pending', 0)}</b> pending · "
        f"{feed.get('processed', 0)} processed · <b>{feed.get('dropped', 0)}</b> dropped",
        f"Event lag <b>{feed.get('lag_ms', 0)} ms</b> (peak {feed.get('max_lag_ms', 0)} ms)",
        f"Mark-price stream {'🟢' if h['markprice_ok'] else '🔴'} "
        f"({h['markprice_symbols']} symbols, {esc(str(h['markprice_age']))})",
        f"Price source <b>{esc(h['price_source'])}</b> · {h['price_symbols']} symbols"
        + (f" · ⚠️ <b>{h['stale_prices']}</b> open setups without a fresh price"
           if h.get("stale_prices") else ""),
        f"Flow streams <b>{h['flow_streams']}</b> · reconnects <b>{h['reconnects']}</b>"
        + (f" · <b>{h['silent_sockets']}</b> silent" if h.get("silent_sockets") else ""),
    ]
    if h.get("rest_polls"):
        lines.append(f"REST flow polls <b>{h['rest_polls']}</b> (websocket feed degraded)")
    lines += [
        f"Database <code>{esc(h['db_path'])}</code> · {h['db_size_mb']:.1f} MB",
        f"Tasks alive <b>{h['tasks']}</b> · Telegram messages sent <b>{h['tg_sent']}</b>",
    ]
    if feed.get("dropped", 0) > 0:
        lines += ["", "<i>Dropped events mean a queue filled faster than it drained. "
                      "Reduce MAX_ARMED_SYMBOLS or raise QUEUE_MAXSIZE.</i>"]
    if not h["markprice_ok"]:
        lines += ["", "<i>The websocket feed is not delivering data. The engine "
                      "is running on REST polling - signals still work, but flow "
                      "resolution is coarser. Often a regional block on the host.</i>"]
    return "\n".join(lines)


def why_message(ctx, decision, trend: str) -> str:
    """Full decision breakdown for one armed symbol - the answer to
    'why is nothing firing?'"""
    d = decision.details
    dec = ctx.decimals
    lines = [
        f"🔍 <b>{esc(ctx.symbol)}</b> {ctx.direction} · zone {ctx.zone.score}"
        f" ({esc(ctx.zone.grade or '-')}) · via {esc(ctx.exchange)}",
        f"4H trend <b>{esc(trend)}</b> · armed {ctx.age_min:.0f}m · "
        f"confidence <b>{decision.confidence:.2f}</b>",
        "",
    ]
    if decision.passed:
        lines.append("✅ <b>All gates passed</b> — a signal should be firing now.")
    else:
        lines.append("<b>Blocked by</b>")
        for b in decision.blockers:
            lines.append(f"⛔ {esc(b)}")

    sweep = d.get("sweep") or {}
    absorb = d.get("absorption") or {}
    bias = d.get("liquidity_bias") or {}
    heat = d.get("heatmap") or {}
    flow = d.get("flow") or {}
    ex = d.get("execution") or {}
    targets = d.get("targets") or []

    lines += [
        "",
        "<b>State</b>",
        f"· flow: {flow.get('trades', 0)} trades / {flow.get('buckets', 0)} buckets, "
        f"{d.get('flow_age', '?')}s old",
        f"· sweep: " + ("none found" if not sweep.get("found") else
                       f"{sweep.get('level', 0):.{dec}f} "
                       f"{sweep.get('age_bars')} bars ago, "
                       f"reclaimed={sweep.get('reclaimed')}, "
                       f"structural={bool(sweep.get('structural'))}"),
        f"· absorption: {absorb.get('ratio', 0)}x, recovery {absorb.get('recovery', 0)}, "
        f"found={absorb.get('found')}",
        f"· liquidity: resting {esc(str(bias.get('bias', '?')))} "
        f"(ratio {bias.get('ratio')}) · heatmap imbalance {heat.get('imbalance')}",
        f"· institutional: {bool(ex.get('institutional'))}",
        f"· confirmations: {d.get('optional_confirms', 0)}",
    ]
    if targets:
        lines.append("· targets: " + ", ".join(
            f"{t['price']:.{dec}f} ({esc(str(t['source']))})" for t in targets[:3]))
    if decision.reasons:
        lines += ["", "<b>Already satisfied</b>"]
        for r in decision.reasons[:6]:
            lines.append(f"✔️ {esc(r)}")
    return "\n".join(lines)


def sleep_message(reason: str, wake, open_trades: int) -> str:
    return "\n".join([
        "😴 <b>WEEKEND SLEEP</b>",
        f"No new setups will be hunted ({esc(reason)}) — weekend books are thin "
        f"and their sweeps rarely follow through.",
        f"Waking <b>{wake.strftime('%A %H:%M')}</b> local time.",
        (f"{open_trades} open setup(s) stay monitored to completion."
         if open_trades else "No open setups."),
    ])


def wake_message(engine: dict) -> str:
    return "\n".join([
        "☀️ <b>AWAKE — HUNTING RESUMED</b>",
        f"Watchlist <b>{engine['watchlist']}</b> · zones <b>{engine['zones']}</b> "
        f"(A+ {engine['a_plus']})",
        "Scanning for setups again.",
    ])


def help_message() -> str:
    return "\n".join([
        "🤖 <b>SNIPER FLOW</b> — order-flow signal engine",
        "",
        "<b>/status</b> — engine + open setups",
        "<b>/active</b> — detailed open setups",
        "<b>/watchlist</b> — volume-filtered universe",
        "<b>/zones SYMBOL</b> — graded S/R zones",
        "<b>/signals [n]</b> — recent signals",
        "<b>/pnl [today|week|month|all]</b> — performance",
        "<b>/report [period]</b> — full report",
        "<b>/stats</b> — internals, armed symbols, rejection reasons",
        "<b>/why SYMBOL</b> — full gate breakdown for an armed symbol",
        "<b>/health</b> — connections, rate limit, memory",
        "<b>/close ID</b> — force-close a setup at market",
        "<b>/pause</b> · <b>/resume</b> — signal generation",
        "",
        "<i>Analysis and alerts only. No orders are ever placed.</i>",
    ])


def startup_message(engine: dict, bot_name: str) -> str:
    return "\n".join([
        "🚀 <b>SNIPER FLOW ONLINE</b>",
        f"Bot {esc(bot_name)}",
        f"Watchlist <b>{engine['watchlist']}</b> symbols · "
        f"volume filter {fmt_usd(engine['min_volume'])}",
        "Feeds: " + " · ".join(f"{esc(k)} {v}" for k, v in
                               (engine.get("exchanges") or {}).items()),
        f"Zone refresh every {engine['zone_refresh']}h · "
        f"proximity scan every {engine['proximity']}s",
        "Send /help for commands.",
    ])

"""
Central configuration.

Every tunable lives here. Values can be overridden from the environment
(.env file is loaded automatically), so you never have to touch the code
on the VPS -- just edit .env and restart.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field, asdict
from typing import Any, Dict

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # pragma: no cover - dotenv is a hard dep, but never crash on it
    pass


def _s(key: str, default: str) -> str:
    v = os.getenv(key)
    return default if v is None or v.strip() == "" else v.strip()


def _f(key: str, default: float) -> float:
    try:
        return float(_s(key, str(default)))
    except ValueError:
        return default


def _i(key: str, default: int) -> int:
    try:
        return int(float(_s(key, str(default))))
    except ValueError:
        return default


def _b(key: str, default: bool) -> bool:
    return _s(key, "true" if default else "false").lower() in ("1", "true", "yes", "y", "on")


@dataclass
class Settings:
    # ---------------------------------------------------------------- telegram
    telegram_token: str = field(default_factory=lambda: _s("TELEGRAM_BOT_TOKEN", ""))
    telegram_chat_id: str = field(default_factory=lambda: _s("TELEGRAM_CHAT_ID", ""))
    telegram_poll_timeout: int = field(default_factory=lambda: _i("TELEGRAM_POLL_TIMEOUT", 25))

    # ------------------------------------------------------------- exchanges
    exchanges: str = field(default_factory=lambda: _s("EXCHANGES", "binance,bybit"))
    history_exchange: str = field(default_factory=lambda: _s("HISTORY_EXCHANGE", "binance"))
    bybit_rest: str = field(default_factory=lambda: _s("BYBIT_REST", "https://api.bybit.com"))
    bybit_ws: str = field(default_factory=lambda: _s("BYBIT_WS", "wss://stream.bybit.com"))
    bybit_budget: int = field(default_factory=lambda: _i("BYBIT_BUDGET_PER_MIN", 500))

    # ------------------------------------------------------------ event bus
    queue_shards: int = field(default_factory=lambda: _i("QUEUE_SHARDS", 2))
    queue_maxsize: int = field(default_factory=lambda: _i("QUEUE_MAXSIZE", 20000))
    queue_batch: int = field(default_factory=lambda: _i("QUEUE_BATCH", 256))
    queue_warn_depth: int = field(default_factory=lambda: _i("QUEUE_WARN_DEPTH", 6000))

    # ---------------------------------------------------------------- binance
    rest_base: str = field(default_factory=lambda: _s("BINANCE_REST", "https://fapi.binance.com"))
    ws_base: str = field(default_factory=lambda: _s("BINANCE_WS", "wss://fstream.binance.com"))
    weight_budget: int = field(default_factory=lambda: _i("WEIGHT_BUDGET_PER_MIN", 1100))
    bulk_weight_share: float = field(default_factory=lambda: _f("BULK_WEIGHT_SHARE", 0.65))  # hard cap 2400
    rest_timeout: int = field(default_factory=lambda: _i("REST_TIMEOUT", 20))

    # ---------------------------------------------------------------- watchlist
    min_quote_volume: float = field(default_factory=lambda: _f("MIN_QUOTE_VOLUME_USD", 20_000_000))
    watchlist_refresh_hours: float = field(default_factory=lambda: _f("WATCHLIST_REFRESH_HOURS", 5))
    max_watchlist: int = field(default_factory=lambda: _i("MAX_WATCHLIST", 220))
    quote_asset: str = field(default_factory=lambda: _s("QUOTE_ASSET", "USDT"))
    blacklist: str = field(default_factory=lambda: _s("SYMBOL_BLACKLIST", ""))
    require_history_listing: bool = field(
        default_factory=lambda: _b("REQUIRE_HISTORY_LISTING", True))
    # Tokenised metals, equities and pre-IPO products. They are quoted in USDT
    # and pass every volume filter, but they track an underlying this engine
    # cannot see, and their books are far too thin for footprint work.
    # Extend this list as venues add more of them.
    exclude_underlyings: str = field(default_factory=lambda: _s(
        "EXCLUDE_UNDERLYINGS",
        "XAU,XAG,XAUT,PAXG,XPT,XPD,WTI,OIL,BRENT,SPX,SPX500,NDX,NAS100,DJI,US30,"
        "DAX,NIKKEI,SPCX,OPENAI,MRVL,NVDA,TSLA,AAPL,MSFT,META,AMZN,GOOGL,GOOG,"
        "NFLX,AMD,INTC,COIN,HOOD,MSTR,PLTR,GME,AMC,BABA,IBIT,CRCL"))

    # ---------------------------------------------------------------- candles
    htf_interval: str = field(default_factory=lambda: _s("HTF_INTERVAL", "4h"))
    mtf_interval: str = field(default_factory=lambda: _s("MTF_INTERVAL", "1h"))
    candle_limit: int = field(default_factory=lambda: _i("CANDLE_LIMIT", 600))
    zone_refresh_hours: float = field(default_factory=lambda: _f("ZONE_REFRESH_HOURS", 4))

    # ---------------------------------------------------------------- zones
    pivot_left: int = field(default_factory=lambda: _i("PIVOT_LEFT", 3))
    pivot_right: int = field(default_factory=lambda: _i("PIVOT_RIGHT", 3))
    cluster_atr_tol: float = field(default_factory=lambda: _f("CLUSTER_ATR_TOL", 0.45))
    zone_min_atr_width: float = field(default_factory=lambda: _f("ZONE_MIN_ATR_WIDTH", 0.22))
    zone_max_atr_width: float = field(default_factory=lambda: _f("ZONE_MAX_ATR_WIDTH", 1.30))
    score_a_plus: int = field(default_factory=lambda: _i("SCORE_A_PLUS", 80))
    score_a: int = field(default_factory=lambda: _i("SCORE_A", 70))
    max_zones_per_symbol: int = field(default_factory=lambda: _i("MAX_ZONES_PER_SYMBOL", 5))
    max_zone_tests: int = field(default_factory=lambda: _i("MAX_ZONE_TESTS", 3))
    structural_min_strength: float = field(default_factory=lambda: _f("STRUCTURAL_MIN_STRENGTH", 0.60))
    equal_level_atr_tol: float = field(default_factory=lambda: _f("EQUAL_LEVEL_ATR_TOL", 0.12))
    structural_major_strength: float = field(
        default_factory=lambda: _f("STRUCTURAL_MAJOR_STRENGTH", 0.70))
    range_position_guard: float = field(default_factory=lambda: _f("RANGE_POSITION_GUARD", 0.45))
    respect_htf_trend: bool = field(default_factory=lambda: _b("RESPECT_HTF_TREND", True))
    counter_trend_policy: str = field(default_factory=lambda: _s("COUNTER_TREND_POLICY", "strict"))
    counter_trend_penalty: float = field(default_factory=lambda: _f("COUNTER_TREND_PENALTY", 0.08))

    # ---------------------------------------------------------------- proximity / arming
    proximity_interval_sec: int = field(default_factory=lambda: _i("PROXIMITY_INTERVAL_SEC", 300))
    arm_buffer_zone_frac: float = field(default_factory=lambda: _f("ARM_BUFFER_ZONE_FRAC", 0.25))
    max_armed_symbols: int = field(default_factory=lambda: _i("MAX_ARMED_SYMBOLS", 18))
    arm_ttl_minutes: int = field(default_factory=lambda: _i("ARM_TTL_MINUTES", 90))
    arm_warmup_sec: int = field(default_factory=lambda: _i("ARM_WARMUP_SEC", 90))
    eval_interval_sec: int = field(default_factory=lambda: _i("EVAL_INTERVAL_SEC", 15))
    rearm_cooldown_minutes: int = field(default_factory=lambda: _i("REARM_COOLDOWN_MINUTES", 120))

    # ---------------------------------------------------------------- order flow
    ltf_fast: str = field(default_factory=lambda: _s("LTF_FAST", "3m"))
    ltf_slow: str = field(default_factory=lambda: _s("LTF_SLOW", "5m"))
    ltf_mid: str = field(default_factory=lambda: _s("LTF_MID", "15m"))
    micro_interval: str = field(default_factory=lambda: _s("MICRO_INTERVAL", "1m"))
    micro_limit: int = field(default_factory=lambda: _i("MICRO_LIMIT", 300))
    ltf_limit: int = field(default_factory=lambda: _i("LTF_LIMIT", 200))
    footprint_bucket_sec: int = field(default_factory=lambda: _i("FOOTPRINT_BUCKET_SEC", 60))
    footprint_window_min: int = field(default_factory=lambda: _i("FOOTPRINT_WINDOW_MIN", 90))
    footprint_price_bins: int = field(default_factory=lambda: _i("FOOTPRINT_PRICE_BINS", 60))
    imbalance_ratio: float = field(default_factory=lambda: _f("IMBALANCE_RATIO", 3.0))
    min_imbalance_stack: int = field(default_factory=lambda: _i("MIN_IMBALANCE_STACK", 2))
    delta_extreme_z: float = field(default_factory=lambda: _f("DELTA_EXTREME_Z", 1.30))
    absorption_vol_mult: float = field(default_factory=lambda: _f("ABSORPTION_VOL_MULT", 1.90))
    absorption_efficiency: float = field(default_factory=lambda: _f("ABSORPTION_EFFICIENCY", 0.45))
    cvd_recovery_frac: float = field(default_factory=lambda: _f("CVD_RECOVERY_FRAC", 0.35))
    min_trades_for_flow: int = field(default_factory=lambda: _i("MIN_TRADES_FOR_FLOW", 400))

    # ------------------------------------------------- liquidity & heatmap
    heatmap_buckets: int = field(default_factory=lambda: _i("HEATMAP_BUCKETS", 400))
    heatmap_half_life_sec: float = field(default_factory=lambda: _f("HEATMAP_HALF_LIFE_SEC", 900))
    heatmap_range_pct: float = field(default_factory=lambda: _f("HEATMAP_RANGE_PCT", 0.035))
    heatmap_target_strength: float = field(default_factory=lambda: _f("HEATMAP_TARGET_STRENGTH", 0.30))
    heatmap_support_pct: float = field(default_factory=lambda: _f("HEATMAP_SUPPORT_PCT", 0.004))
    liquidity_span_pct: float = field(default_factory=lambda: _f("LIQUIDITY_SPAN_PCT", 0.08))
    liquidity_bias_min: float = field(default_factory=lambda: _f("LIQUIDITY_BIAS_MIN", 0.50))
    target_min_distance_pct: float = field(default_factory=lambda: _f("TARGET_MIN_DISTANCE_PCT", 0.0015))
    target_merge_pct: float = field(default_factory=lambda: _f("TARGET_MERGE_PCT", 0.0012))

    # -------------------------------------------------- execution algorithms
    algo_window_sec: int = field(default_factory=lambda: _i("ALGO_WINDOW_SEC", 900))
    algo_min_clips: int = field(default_factory=lambda: _i("ALGO_MIN_CLIPS", 12))
    algo_regularity_max: float = field(default_factory=lambda: _f("ALGO_REGULARITY_MAX", 0.35))
    iceberg_ratio: float = field(default_factory=lambda: _f("ICEBERG_RATIO", 2.5))
    require_institutional: bool = field(default_factory=lambda: _b("REQUIRE_INSTITUTIONAL", False))
    vwap_lookback: int = field(default_factory=lambda: _i("VWAP_LOOKBACK", 240))
    derived_refresh_sec: float = field(default_factory=lambda: _f("DERIVED_REFRESH_SEC", 10))

    # ---------------------------------------------------------------- order book
    depth_levels: int = field(default_factory=lambda: _i("DEPTH_LEVELS", 20))
    depth_speed_ms: int = field(default_factory=lambda: _i("DEPTH_SPEED_MS", 500))
    ob_snapshots_keep: int = field(default_factory=lambda: _i("OB_SNAPSHOTS_KEEP", 240))
    ob_wall_mult: float = field(default_factory=lambda: _f("OB_WALL_MULT", 3.0))
    ob_pull_ratio_max: float = field(default_factory=lambda: _f("OB_PULL_RATIO_MAX", 0.65))
    ob_stack_ratio: float = field(default_factory=lambda: _f("OB_STACK_RATIO", 1.35))

    # ---------------------------------------------------------------- structure
    sweep_lookback_bars: int = field(default_factory=lambda: _i("SWEEP_LOOKBACK_BARS", 60))
    sweep_max_age_bars: int = field(default_factory=lambda: _i("SWEEP_MAX_AGE_BARS", 5))
    sweep_fresh_bars: int = field(default_factory=lambda: _i("SWEEP_FRESH_BARS", 3))
    sweep_wick_bars: int = field(default_factory=lambda: _i("SWEEP_WICK_BARS", 3))
    require_structural_sweep: bool = field(default_factory=lambda: _b("REQUIRE_STRUCTURAL_SWEEP", True))
    sweep_min_pierce_atr: float = field(default_factory=lambda: _f("SWEEP_MIN_PIERCE_ATR", 0.05))
    mss_lookback_bars: int = field(default_factory=lambda: _i("MSS_LOOKBACK_BARS", 40))

    # ---------------------------------------------------------------- decision
    min_confidence: float = field(default_factory=lambda: _f("MIN_CONFIDENCE", 0.65))
    min_optional_confirms: int = field(default_factory=lambda: _i("MIN_OPTIONAL_CONFIRMS", 3))

    # ---------------------------------------------------------------- risk / targets
    sl_buffer_atr: float = field(default_factory=lambda: _f("SL_BUFFER_ATR", 0.35))
    sl_buffer_pct_min: float = field(default_factory=lambda: _f("SL_BUFFER_PCT_MIN", 0.0010))
    entry_pad_atr: float = field(default_factory=lambda: _f("ENTRY_PAD_ATR", 0.12))
    tp1_r: float = field(default_factory=lambda: _f("TP1_R", 1.0))
    tp1_min_r: float = field(default_factory=lambda: _f("TP1_MIN_R", 0.85))
    tp2_min_r: float = field(default_factory=lambda: _f("TP2_MIN_R", 1.70))
    tp3_min_r: float = field(default_factory=lambda: _f("TP3_MIN_R", 2.80))
    sl_zone_edge_atr: float = field(default_factory=lambda: _f("SL_ZONE_EDGE_ATR", 1.2))
    tp2_r: float = field(default_factory=lambda: _f("TP2_R", 2.0))
    tp3_r: float = field(default_factory=lambda: _f("TP3_R", 3.5))
    min_risk_pct: float = field(default_factory=lambda: _f("MIN_RISK_PCT", 0.0018))
    max_risk_pct: float = field(default_factory=lambda: _f("MAX_RISK_PCT", 0.045))
    structural_tp_cap: bool = field(default_factory=lambda: _b("STRUCTURAL_TP_CAP", True))
    min_rr_after_cap: float = field(default_factory=lambda: _f("MIN_RR_AFTER_CAP", 2.0))

    # ---------------------------------------------------------------- trade monitor
    max_active_trades: int = field(default_factory=lambda: _i("MAX_ACTIVE_TRADES", 8))
    breakeven_after_tp1: bool = field(default_factory=lambda: _b("BREAKEVEN_AFTER_TP1", True))
    trail_to_tp1_after_tp2: bool = field(default_factory=lambda: _b("TRAIL_TO_TP1_AFTER_TP2", True))
    trade_ttl_hours: float = field(default_factory=lambda: _f("TRADE_TTL_HOURS", 48))
    monitor_tick_sec: int = field(default_factory=lambda: _i("MONITOR_TICK_SEC", 2))

    # ---------------------------------------------------------------- system
    db_path: str = field(default_factory=lambda: _s("DB_PATH", "data/sniper.db"))
    log_level: str = field(default_factory=lambda: _s("LOG_LEVEL", "INFO"))
    log_file: str = field(default_factory=lambda: _s("LOG_FILE", "data/sniper.log"))
    housekeeping_minutes: int = field(default_factory=lambda: _i("HOUSEKEEPING_MINUTES", 15))
    startup_notice: bool = field(default_factory=lambda: _b("STARTUP_NOTICE", True))

    # ------------------------------------------------ resilience / transport
    # Some networks accept the WebSocket handshake and then deliver nothing at
    # all. These control how fast that is detected and how the engine copes.
    ws_idle_timeout_sec: int = field(default_factory=lambda: _i("WS_IDLE_TIMEOUT_SEC", 45))
    markprice_give_up: int = field(default_factory=lambda: _i("MARKPRICE_GIVE_UP_AFTER", 8))
    ws_flow_idle_timeout_sec: int = field(default_factory=lambda: _i("WS_FLOW_IDLE_TIMEOUT_SEC", 120))
    max_flow_age_sec: int = field(default_factory=lambda: _i("MAX_FLOW_AGE_SEC", 240))
    rest_fallback: bool = field(default_factory=lambda: _b("REST_FALLBACK", True))
    flow_poll_sec: int = field(default_factory=lambda: _i("FLOW_POLL_SEC", 20))
    max_armed_fallback: int = field(default_factory=lambda: _i("MAX_ARMED_FALLBACK", 4))
    price_cache_sec: float = field(default_factory=lambda: _f("PRICE_CACHE_SEC", 5.0))
    price_poll_sec: float = field(default_factory=lambda: _f("PRICE_POLL_SEC", 2.0))
    command_timeout_sec: int = field(default_factory=lambda: _i("COMMAND_TIMEOUT_SEC", 45))

    # ------------------------------------------------------------------ helpers
    @property
    def excluded_underlyings(self) -> set:
        return {s.strip().upper() for s in self.exclude_underlyings.split(",") if s.strip()}

    @property
    def exchange_list(self) -> list:
        return [e.strip().lower() for e in self.exchanges.split(",") if e.strip()]

    @property
    def blacklist_set(self) -> set:
        return {s.strip().upper() for s in self.blacklist.split(",") if s.strip()}

    def validate(self) -> list:
        problems = []
        if not self.telegram_token:
            problems.append("TELEGRAM_BOT_TOKEN is missing")
        if not self.telegram_chat_id:
            problems.append("TELEGRAM_CHAT_ID is missing")
        if self.weight_budget > 2000:
            problems.append("WEIGHT_BUDGET_PER_MIN above 2000 risks an IP ban (max safe ~1800)")
        if self.score_a > self.score_a_plus:
            problems.append("SCORE_A must be <= SCORE_A_PLUS")
        if not self.exchange_list:
            problems.append("EXCHANGES must list at least one venue")
        if self.history_exchange not in self.exchange_list:
            problems.append(f"HISTORY_EXCHANGE '{self.history_exchange}' is not in EXCHANGES")
        if self.counter_trend_policy not in ("allow", "strict", "block"):
            problems.append("COUNTER_TREND_POLICY must be allow|strict|block")
        return problems

    def as_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["telegram_token"] = "***" if self.telegram_token else ""
        return d


SETTINGS = Settings()

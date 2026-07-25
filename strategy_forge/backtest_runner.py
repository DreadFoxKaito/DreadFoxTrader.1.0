from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date
from statistics import mean
from typing import Any, Optional

from .data_loader import OHLCVData, parse_timestamp
from .indicator_factory import IndicatorCache, is_valid
from .strategy_templates import StrategyCandidate


@dataclass
class BacktestConfig:
    initial_capital: float = 100_000.0
    commission_pct: float = 0.0005
    commission_flat: float = 0.0
    slippage_bps: float = 2.0
    max_position_pct: float = 0.15
    default_position_pct: float = 0.10
    max_daily_loss_pct: float = 0.03
    max_trades_per_day: int = 8
    regular_hours_only: bool = False
    close_open_position: bool = True
    use_next_open: bool = True


@dataclass
class BacktestResult:
    candidate: StrategyCandidate
    symbol: str
    timeframe: str
    metrics: dict[str, Any]
    trades: list[dict[str, Any]]
    equity_curve: list[dict[str, Any]]
    monthly_returns: dict[str, float] = field(default_factory=dict)
    regime_summary: dict[str, dict[str, Any]] = field(default_factory=dict)


def periods_per_year(timeframe: str) -> float:
    tf = str(timeframe or "").lower()
    if tf.endswith("m"):
        minutes = max(1, int(tf[:-1] or 1))
        return (252.0 * 390.0) / float(minutes)
    if tf.endswith("h"):
        hours = max(1, int(tf[:-1] or 1))
        return (252.0 * 6.5) / float(hours)
    if tf.endswith("d"):
        return 252.0
    return 252.0


def max_drawdown(equity_values: list[float]) -> float:
    peak = None
    worst = 0.0
    for value in equity_values:
        v = float(value)
        if peak is None or v > peak:
            peak = v
        if peak and peak > 0:
            worst = max(worst, (peak - v) / peak)
    return worst


def profit_factor(trade_pnls: list[float]) -> float:
    gross_profit = sum(v for v in trade_pnls if v > 0)
    gross_loss = abs(sum(v for v in trade_pnls if v < 0))
    if gross_loss <= 0:
        return 999.0 if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def _safe_ratio(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def _pct_returns(values: list[float]) -> list[float]:
    out: list[float] = []
    for i in range(1, len(values)):
        prev = float(values[i - 1])
        cur = float(values[i])
        if prev > 0:
            out.append((cur / prev) - 1.0)
    return out


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    return math.sqrt(sum((v - avg) ** 2 for v in values) / float(len(values) - 1))


def sharpe_ratio(returns: list[float], timeframe: str) -> float:
    if not returns:
        return 0.0
    sd = _std(returns)
    if sd <= 0:
        return 0.0
    return (mean(returns) / sd) * math.sqrt(periods_per_year(timeframe))


def sortino_ratio(returns: list[float], timeframe: str) -> float:
    if not returns:
        return 0.0
    downside = [r for r in returns if r < 0]
    sd = _std(downside)
    if sd <= 0:
        return 0.0 if mean(returns) <= 0 else 10.0
    return (mean(returns) / sd) * math.sqrt(periods_per_year(timeframe))


def _day_key(ts: str) -> str:
    parsed = parse_timestamp(ts)
    return parsed.date().isoformat() if parsed else str(ts)[:10]


def _month_key(ts: str) -> str:
    parsed = parse_timestamp(ts)
    if parsed:
        return f"{parsed.year:04d}-{parsed.month:02d}"
    return str(ts)[:7] or "unknown"


def _is_regular_session(session: str) -> bool:
    s = str(session or "").strip().lower()
    return s in ("", "regular", "reg", "regular_session")


def _value(indicators: dict[str, Any], name: str, index: int) -> float:
    series = indicators.get(name)
    if series is None:
        return float("nan")
    try:
        return float(series[index])
    except Exception:
        return float("nan")


def _is_combo_candidate(candidate: StrategyCandidate) -> bool:
    return (
        str(candidate.strategy_name or "").lower() == "indicator_combo_evolved"
        or str(candidate.parameters.get("strategy_kind") or "").lower() == "open_indicator_combo"
    )


def _combo_rules(candidate: StrategyCandidate) -> list[dict[str, Any]]:
    return [rule for rule in (candidate.parameters.get("rules") or []) if isinstance(rule, dict)]


def _combo_rule_id(rule: dict[str, Any], index: int) -> str:
    return str(rule.get("id") or f"{rule.get('kind') or 'rule'}_{index + 1}")


def _combo_rule_timeframe(rule: dict[str, Any], default_timeframe: str) -> str:
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    tf = str(rule.get("timeframe") or params.get("timeframe") or default_timeframe or "").strip().lower()
    aliases = {
        "1min": "1m",
        "1minute": "1m",
        "5min": "5m",
        "5minute": "5m",
        "10min": "10m",
        "10minute": "10m",
        "15min": "15m",
        "15minute": "15m",
        "30min": "30m",
        "30minute": "30m",
        "60m": "1h",
        "1hour": "1h",
        "hour": "1h",
        "1day": "1d",
        "day": "1d",
        "daily": "1d",
    }
    return aliases.get(tf, tf or str(default_timeframe or "1h").strip().lower())


def _combo_rule_timeframes(candidate: StrategyCandidate, default_timeframe: str) -> list[str]:
    out: list[str] = []
    for rule in _combo_rules(candidate):
        tf = _combo_rule_timeframe(rule, default_timeframe)
        if tf not in out:
            out.append(tf)
    return out or [str(default_timeframe or "1h").strip().lower()]


def _timestamp_seconds(data: OHLCVData) -> list[Optional[float]]:
    out: list[Optional[float]] = []
    for ts in data.timestamps:
        parsed = parse_timestamp(ts)
        out.append(parsed.timestamp() if parsed else None)
    return out


def _build_timeframe_alignment(execution_data: OHLCVData, rule_data: OHLCVData) -> list[int]:
    if len(rule_data) <= 0:
        return []
    if rule_data is execution_data or (
        str(rule_data.timeframe).lower() == str(execution_data.timeframe).lower()
        and list(rule_data.timestamps) == list(execution_data.timestamps)
    ):
        return [min(i, len(rule_data) - 1) for i in range(len(execution_data))]

    rule_times = _timestamp_seconds(rule_data)
    exec_times = _timestamp_seconds(execution_data)
    valid_pairs = [(float(ts), idx) for idx, ts in enumerate(rule_times) if ts is not None]
    if not valid_pairs or not any(ts is not None for ts in exec_times):
        return [
            min(len(rule_data) - 1, int(round((i / max(1, len(execution_data) - 1)) * (len(rule_data) - 1))))
            for i in range(len(execution_data))
        ]

    valid_pairs.sort(key=lambda item: item[0])
    sorted_times = [item[0] for item in valid_pairs]
    sorted_indices = [item[1] for item in valid_pairs]
    alignment: list[int] = []
    for i, ts in enumerate(exec_times):
        if ts is None:
            alignment.append(min(i, len(rule_data) - 1))
            continue
        pos = bisect_right(sorted_times, float(ts)) - 1
        alignment.append(-1 if pos < 0 else sorted_indices[pos])
    return alignment


def _combo_context_for_rule(indicators: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
    default_tf = str(indicators.get("execution_timeframe") or "").strip().lower()
    tf = _combo_rule_timeframe(rule, default_tf)
    by_tf = indicators.get("combo_by_timeframe")
    if isinstance(by_tf, dict):
        ctx = by_tf.get(tf)
        if isinstance(ctx, dict):
            return ctx
    return {"data": indicators.get("execution_data"), "combo": indicators.get("combo") or {}, "alignment": None}


def _combo_data_and_index(
    data: OHLCVData,
    indicators: dict[str, Any],
    rule: dict[str, Any],
    execution_index: int,
) -> tuple[OHLCVData, int]:
    ctx = _combo_context_for_rule(indicators, rule)
    rule_data = ctx.get("data")
    rule_data = rule_data if isinstance(rule_data, OHLCVData) else data
    alignment = ctx.get("alignment")
    if isinstance(alignment, list) and 0 <= int(execution_index) < len(alignment):
        idx = int(alignment[int(execution_index)])
    else:
        idx = int(execution_index)
    idx = max(0, min(idx, len(rule_data) - 1 if len(rule_data) else 0))
    return rule_data, idx


def _combo_value(indicators: dict[str, Any], rule: dict[str, Any], key: str, index: int) -> float:
    ctx = _combo_context_for_rule(indicators, rule)
    combo = ctx.get("combo")
    if not isinstance(combo, dict):
        return float("nan")
    values = combo.get(str(rule.get("id") or ""))
    if not isinstance(values, dict):
        return float("nan")
    series = values.get(key)
    if series is None:
        return float("nan")
    alignment = ctx.get("alignment")
    value_index = int(index)
    if isinstance(alignment, list) and 0 <= int(index) < len(alignment):
        value_index = int(alignment[int(index)])
    if value_index < 0:
        return float("nan")
    try:
        return float(series[value_index])
    except Exception:
        return float("nan")


def _compute_indicators(
    data: OHLCVData,
    candidate: StrategyCandidate,
    *,
    data_by_timeframe: Optional[dict[str, OHLCVData]] = None,
) -> dict[str, Any]:
    p = candidate.parameters
    cache = IndicatorCache(data)
    out: dict[str, Any] = {
        "regimes": cache.get("regimes"),
    }
    name = candidate.strategy_name
    ma_type = str(p.get("ma_type") or "ema")
    if _is_combo_candidate(candidate):
        out["atr"] = cache.get("atr", length=int(p.get("atr_length") or 14))
        out["execution_data"] = data
        out["execution_timeframe"] = str(data.timeframe or candidate.timeframe or "").strip().lower()
        out["combo"] = {}
        sources: dict[str, OHLCVData] = {str(data.timeframe).strip().lower(): data}
        for tf, tf_data in (data_by_timeframe or {}).items():
            if isinstance(tf_data, OHLCVData) and len(tf_data) > 0:
                sources[str(tf or tf_data.timeframe).strip().lower()] = tf_data
                sources[str(tf_data.timeframe).strip().lower()] = tf_data
        out["combo_by_timeframe"] = {}
        for tf in _combo_rule_timeframes(candidate, str(data.timeframe or candidate.timeframe or "1h")):
            tf_data = sources.get(tf) or data
            tf_cache = cache if tf_data is data else IndicatorCache(tf_data)
            combo_values: dict[str, Any] = {}
            for idx, rule in enumerate(_combo_rules(candidate)):
                rule_id = _combo_rule_id(rule, idx)
                rule["id"] = rule_id
                if _combo_rule_timeframe(rule, str(data.timeframe or candidate.timeframe or "1h")) != tf:
                    continue
                kind = str(rule.get("kind") or "").lower()
                params = dict(rule.get("params") or {})
                values: dict[str, Any] = {}
                if kind == "ma_cross":
                    values["fast_ma"] = tf_cache.get(
                        "ma",
                        length=int(params.get("fast") or 12),
                        ma_type=str(params.get("ma_type") or "ema"),
                    )
                    values["slow_ma"] = tf_cache.get(
                        "ma",
                        length=int(params.get("slow") or 26),
                        ma_type=str(params.get("ma_type") or "ema"),
                    )
                elif kind == "rsi_momentum":
                    values["rsi"] = tf_cache.get("rsi", length=int(params.get("length") or 14))
                elif kind == "rsi_derivative":
                    values["rsi_derivative"] = tf_cache.get("rsi_derivative", length=int(params.get("length") or 14))
                elif kind == "macd_momentum":
                    values["macd"], values["macd_signal"], values["macd_hist"] = tf_cache.get(
                        "macd",
                        fast=int(params.get("fast") or 12),
                        slow=int(params.get("slow") or 26),
                        signal=int(params.get("signal") or 9),
                    )
                elif kind in {"bollinger_pullback", "bollinger_breakout"}:
                    values["bb_middle"], values["bb_upper"], values["bb_lower"], values["percent_b"] = tf_cache.get(
                        "bollinger",
                        length=int(params.get("length") or 20),
                        std_mult=float(params.get("std_mult") or 2.0),
                    )
                elif kind == "donchian_breakout":
                    values["donchian_high"], values["donchian_low"] = tf_cache.get(
                        "donchian",
                        lookback=int(params.get("lookback") or 20),
                    )
                elif kind == "supertrend_trend":
                    values["supertrend"], values["supertrend_direction"] = tf_cache.get(
                        "supertrend",
                        atr_length=int(params.get("atr_length") or 10),
                        multiplier=float(params.get("multiplier") or 3.0),
                    )
                elif kind == "vwap_filter":
                    values["vwap"] = tf_cache.get("vwap")
                elif kind == "relative_volume":
                    values["relative_volume"] = tf_cache.get("relative_volume", length=int(params.get("length") or 20))
                elif kind == "roc_momentum":
                    values["roc"] = tf_cache.get("roc", length=int(params.get("length") or 12))
                elif kind == "sar_trend":
                    values["sar"], values["sar_direction"] = tf_cache.get(
                        "sar",
                        step=float(params.get("step") or 0.02),
                        max_step=float(params.get("max_step") or 0.2),
                    )
                elif kind == "ichimoku_trend":
                    values.update(
                        tf_cache.get(
                            "ichimoku",
                            tenkan=int(params.get("tenkan") or 9),
                            kijun=int(params.get("kijun") or 26),
                            senkou_b=int(params.get("senkou_b") or 52),
                        )
                    )
                elif kind == "ttm_squeeze":
                    values.update(
                        tf_cache.get(
                            "ttm",
                            bb_length=int(params.get("bb_length") or 20),
                            bb_mult=float(params.get("bb_mult") or 2.0),
                            kc_length=int(params.get("kc_length") or 20),
                            kc_mult=float(params.get("kc_mult") or 1.5),
                            momentum_length=int(params.get("momentum_length") or 20),
                        )
                    )
                elif kind == "heikin_ashi_trend":
                    values.update(tf_cache.get("heikin_ashi"))
                combo_values[rule_id] = values
            alignment = None if tf_data is data else _build_timeframe_alignment(data, tf_data)
            out["combo_by_timeframe"][tf] = {
                "data": tf_data,
                "combo": combo_values,
                "alignment": alignment,
            }
        exec_tf = str(data.timeframe).strip().lower()
        exec_ctx = out["combo_by_timeframe"].get(exec_tf)
        if isinstance(exec_ctx, dict) and isinstance(exec_ctx.get("combo"), dict):
            out["combo"] = exec_ctx["combo"]
        return out

    if name in {"ema_rsi_atr_trend", "ema_macd_atr_trend"}:
        out["ma_fast"] = cache.get("ma", length=int(p["ma_fast"]), ma_type=ma_type)
        out["ma_slow"] = cache.get("ma", length=int(p["ma_slow"]), ma_type=ma_type)
    if name == "ema_rsi_atr_trend":
        out["rsi"] = cache.get("rsi", length=int(p["rsi_length"]))
        out["atr"] = cache.get("atr", length=int(p["atr_length"]))
    elif name == "ema_macd_atr_trend":
        out["macd"], out["macd_signal"], out["macd_hist"] = cache.get(
            "macd",
            fast=int(p["macd_fast"]),
            slow=int(p["macd_slow"]),
            signal=int(p["macd_signal"]),
        )
        out["atr"] = cache.get("atr", length=int(p["atr_length"]))
    elif name == "bollinger_trend_mean_reversion":
        out["bb_middle"], out["bb_upper"], out["bb_lower"], out["percent_b"] = cache.get(
            "bollinger",
            length=int(p["bb_length"]),
            std_mult=float(p["bb_std"]),
        )
        out["trend_ma"] = cache.get("ma", length=int(p["trend_ma_length"]), ma_type="ema")
        out["atr"] = cache.get("atr", length=int(p["atr_length"]))
    elif name == "vwap_pullback_volume":
        out["vwap"] = cache.get("vwap")
        out["relative_volume"] = cache.get("relative_volume", length=20)
        out["trend_ma"] = cache.get("ma", length=int(p["trend_ma_length"]), ma_type="ema")
        out["atr"] = cache.get("atr", length=int(p["atr_length"]))
    elif name == "donchian_atr_breakout":
        out["donchian_high"], out["donchian_low"] = cache.get("donchian", lookback=int(p["donchian_lookback"]))
        out["atr"] = cache.get("atr", length=int(p["atr_length"]))
    elif name == "ichimoku_cloud_trend":
        out.update(
            cache.get(
                "ichimoku",
                tenkan=int(p["tenkan_period"]),
                kijun=int(p["kijun_period"]),
                senkou_b=int(p["senkou_b_period"]),
            )
        )
        out["atr"] = cache.get("atr", length=int(p["atr_length"]))
    elif name == "supertrend_rsi_confirmation":
        out["supertrend"], out["supertrend_direction"] = cache.get(
            "supertrend",
            atr_length=int(p["supertrend_atr_length"]),
            multiplier=float(p["supertrend_multiplier"]),
        )
        out["rsi"] = cache.get("rsi", length=int(p["rsi_length"]))
        out["atr"] = cache.get("atr", length=int(p["atr_length"]))
    return out


def _atr_derivative(indicators: dict[str, Any], index: int) -> float:
    atr_now = _value(indicators, "atr", index)
    atr_prev = _value(indicators, "atr", index - 1) if index > 0 else float("nan")
    if not is_valid(atr_now) or not is_valid(atr_prev):
        return float("nan")
    return atr_now - atr_prev


def _combo_roc_condition_hit(cond: str, roc: float, prev_roc: float, threshold: float) -> bool:
    if not is_valid(roc):
        return False
    c = str(cond or "").strip().lower()
    has_prev = is_valid(prev_roc)
    if c == "roc_above_threshold":
        return roc >= float(threshold)
    if c == "roc_below_threshold":
        return roc <= float(threshold)
    if c == "roc_cross_up_zero":
        return has_prev and prev_roc <= 0.0 < roc
    if c == "roc_cross_down_zero":
        return has_prev and prev_roc >= 0.0 > roc
    if c == "roc_cross_up_threshold":
        return has_prev and prev_roc <= float(threshold) < roc
    if c == "roc_cross_down_threshold":
        return has_prev and prev_roc >= float(threshold) > roc
    if c == "roc_increasing":
        return has_prev and roc > prev_roc
    if c == "roc_decreasing":
        return has_prev and roc < prev_roc
    if c == "roc_positive":
        return roc > 0.0
    if c == "roc_negative":
        return roc < 0.0
    if c == "momentum_long":
        return has_prev and roc > 0.0 and roc > prev_roc
    if c == "momentum_short":
        return has_prev and roc < 0.0 and roc < prev_roc
    return False


def _combo_sar_condition_hit(
    cond: str,
    *,
    close: float,
    prev_close: float,
    sar: float,
    prev_sar: float,
    direction: float,
) -> bool:
    if not is_valid(sar):
        return False
    c = str(cond or "").strip().lower()
    has_prev = is_valid(prev_close) and is_valid(prev_sar)
    if c == "price_above_sar":
        return close > sar
    if c == "price_below_sar":
        return close < sar
    if c == "sar_cross_up":
        return has_prev and prev_close <= prev_sar and close > sar
    if c == "sar_cross_down":
        return has_prev and prev_close >= prev_sar and close < sar
    if c == "sar_rising":
        return is_valid(prev_sar) and sar > prev_sar
    if c == "sar_falling":
        return is_valid(prev_sar) and sar < prev_sar
    if c == "trend_long":
        return is_valid(direction) and direction > 0.0 and close > sar
    if c == "trend_short":
        return is_valid(direction) and direction < 0.0 and close < sar
    return False


def _combo_ichimoku_condition_hit(
    cond: str,
    *,
    close: float,
    prev_close: float,
    tenkan: float,
    prev_tenkan: float,
    kijun: float,
    prev_kijun: float,
    span_a: float,
    span_b: float,
    cloud_top: float,
    cloud_bottom: float,
) -> bool:
    c = str(cond or "").strip().lower()
    has_cloud = is_valid(cloud_top) and is_valid(cloud_bottom)
    has_cross = is_valid(prev_tenkan) and is_valid(prev_kijun)
    cloud_bullish = is_valid(span_a) and is_valid(span_b) and span_a > span_b
    cloud_bearish = is_valid(span_a) and is_valid(span_b) and span_a < span_b
    if c == "price_above_cloud":
        return has_cloud and close > cloud_top
    if c == "price_below_cloud":
        return has_cloud and close < cloud_bottom
    if c == "cloud_bullish":
        return cloud_bullish
    if c == "cloud_bearish":
        return cloud_bearish
    if c == "tenkan_above_kijun":
        return is_valid(tenkan) and is_valid(kijun) and tenkan > kijun
    if c == "tenkan_below_kijun":
        return is_valid(tenkan) and is_valid(kijun) and tenkan < kijun
    if c == "tenkan_cross_above":
        return has_cross and prev_tenkan <= prev_kijun and tenkan > kijun
    if c == "tenkan_cross_below":
        return has_cross and prev_tenkan >= prev_kijun and tenkan < kijun
    if c == "full_bullish_stack":
        return is_valid(tenkan) and is_valid(kijun) and close > tenkan > kijun
    if c == "full_bearish_stack":
        return is_valid(tenkan) and is_valid(kijun) and close < tenkan < kijun
    if c == "partial_bullish_stack":
        checks = [
            is_valid(tenkan) and close > tenkan,
            is_valid(tenkan) and is_valid(kijun) and tenkan > kijun,
            has_cloud and close > cloud_top,
        ]
        return sum(1 for ok in checks if ok) >= 2
    if c == "partial_bearish_stack":
        checks = [
            is_valid(tenkan) and close < tenkan,
            is_valid(tenkan) and is_valid(kijun) and tenkan < kijun,
            has_cloud and close < cloud_bottom,
        ]
        return sum(1 for ok in checks if ok) >= 2
    if c == "strong_long_confirm":
        return (
            is_valid(tenkan)
            and is_valid(kijun)
            and has_cloud
            and tenkan > kijun
            and close > cloud_top
            and (cloud_bullish or not is_valid(span_a) or not is_valid(span_b))
        )
    if c == "strong_short_confirm":
        return (
            is_valid(tenkan)
            and is_valid(kijun)
            and has_cloud
            and tenkan < kijun
            and close < cloud_bottom
            and (cloud_bearish or not is_valid(span_a) or not is_valid(span_b))
        )
    return False


def _combo_ttm_condition_hit(cond: str, indicators: dict[str, Any], rule: dict[str, Any], index: int) -> bool:
    c = str(cond or "").strip().lower()
    squeeze_on = _combo_value(indicators, rule, "ttm_squeeze_on", index)
    squeeze_fired = _combo_value(indicators, rule, "ttm_squeeze_fired", index)
    momentum = _combo_value(indicators, rule, "ttm_momentum", index)
    prev_momentum = _combo_value(indicators, rule, "ttm_momentum", index - 1) if index > 0 else float("nan")
    momentum_delta = _combo_value(indicators, rule, "ttm_momentum_delta", index)
    if c == "squeeze_on":
        return is_valid(squeeze_on) and squeeze_on > 0.0
    if c == "squeeze_off":
        return is_valid(squeeze_on) and squeeze_on <= 0.0
    if c == "squeeze_fired":
        return is_valid(squeeze_fired) and squeeze_fired > 0.0
    if c == "momentum_above_zero":
        return is_valid(momentum) and momentum > 0.0
    if c == "momentum_below_zero":
        return is_valid(momentum) and momentum < 0.0
    if c == "momentum_increasing":
        return is_valid(momentum_delta) and momentum_delta > 0.0
    if c == "momentum_decreasing":
        return is_valid(momentum_delta) and momentum_delta < 0.0
    if c == "momentum_cross_up":
        return is_valid(prev_momentum) and is_valid(momentum) and prev_momentum <= 0.0 < momentum
    if c == "momentum_cross_down":
        return is_valid(prev_momentum) and is_valid(momentum) and prev_momentum >= 0.0 > momentum
    if c == "long_trend":
        return (
            is_valid(squeeze_on)
            and squeeze_on <= 0.0
            and is_valid(momentum)
            and momentum > 0.0
            and is_valid(momentum_delta)
            and momentum_delta > 0.0
        )
    if c == "short_trend":
        return (
            is_valid(squeeze_on)
            and squeeze_on <= 0.0
            and is_valid(momentum)
            and momentum < 0.0
            and is_valid(momentum_delta)
            and momentum_delta < 0.0
        )
    if c == "long_release":
        return is_valid(squeeze_fired) and squeeze_fired > 0.0 and is_valid(momentum) and momentum > 0.0
    if c == "short_release":
        return is_valid(squeeze_fired) and squeeze_fired > 0.0 and is_valid(momentum) and momentum < 0.0
    return False


def _combo_rule_entry_signal(rule: dict[str, Any], data: OHLCVData, indicators: dict[str, Any], index: int) -> bool:
    kind = str(rule.get("kind") or "").lower()
    params = dict(rule.get("params") or {})
    rule_data, rule_index = _combo_data_and_index(data, indicators, rule, index)
    close = float(rule_data.closes[rule_index])
    if kind == "ma_cross":
        fast = _combo_value(indicators, rule, "fast_ma", index)
        slow = _combo_value(indicators, rule, "slow_ma", index)
        return is_valid(fast) and is_valid(slow) and close > fast and fast > slow
    if kind == "rsi_momentum":
        rsi_now = _combo_value(indicators, rule, "rsi", index)
        return is_valid(rsi_now) and rsi_now >= float(params.get("entry_min") or 55)
    if kind == "rsi_derivative":
        drsi = _combo_value(indicators, rule, "rsi_derivative", index)
        return is_valid(drsi) and drsi >= float(params.get("buy_above") or 0.0)
    if kind == "macd_momentum":
        macd_now = _combo_value(indicators, rule, "macd", index)
        signal = _combo_value(indicators, rule, "macd_signal", index)
        hist = _combo_value(indicators, rule, "macd_hist", index)
        return (
            is_valid(macd_now)
            and is_valid(signal)
            and is_valid(hist)
            and macd_now > signal
            and hist >= float(params.get("hist_min") or 0.0)
        )
    if kind == "bollinger_pullback":
        percent_b = _combo_value(indicators, rule, "percent_b", index)
        return is_valid(percent_b) and percent_b <= float(params.get("entry_b") or 0.25)
    if kind == "bollinger_breakout":
        percent_b = _combo_value(indicators, rule, "percent_b", index)
        return is_valid(percent_b) and percent_b >= float(params.get("entry_b") or 0.85)
    if kind == "donchian_breakout":
        high_channel = _combo_value(indicators, rule, "donchian_high", index)
        if not is_valid(high_channel):
            return False
        if bool(params.get("use_high_break")):
            return float(rule_data.highs[rule_index]) > high_channel
        return close > high_channel
    if kind == "supertrend_trend":
        trend = _combo_value(indicators, rule, "supertrend", index)
        direction = _combo_value(indicators, rule, "supertrend_direction", index)
        return is_valid(trend) and is_valid(direction) and close > trend and direction > 0
    if kind == "vwap_filter":
        vw = _combo_value(indicators, rule, "vwap", index)
        if not is_valid(vw) or vw <= 0:
            return False
        max_extension = float(params.get("max_extension_pct") or 0.015)
        max_pullback = float(params.get("max_pullback_pct") or 0.01)
        return close >= vw * (1.0 - max_pullback) and close <= vw * (1.0 + max_extension)
    if kind == "relative_volume":
        rv = _combo_value(indicators, rule, "relative_volume", index)
        return is_valid(rv) and rv >= float(params.get("threshold") or 1.2)
    if kind == "roc_momentum":
        roc = _combo_value(indicators, rule, "roc", index)
        prev_roc = _combo_value(indicators, rule, "roc", index - 1) if index > 0 else float("nan")
        return _combo_roc_condition_hit(
            str(params.get("buy_condition") or "momentum_long"),
            roc,
            prev_roc,
            float(params.get("buy_threshold_pct") or 0.0),
        )
    if kind == "sar_trend":
        sar = _combo_value(indicators, rule, "sar", index)
        prev_sar = _combo_value(indicators, rule, "sar", index - 1) if index > 0 else float("nan")
        prev_close = float(rule_data.closes[max(0, rule_index - 1)]) if rule_index > 0 else float("nan")
        direction = _combo_value(indicators, rule, "sar_direction", index)
        return _combo_sar_condition_hit(
            str(params.get("buy_condition") or "trend_long"),
            close=close,
            prev_close=prev_close,
            sar=sar,
            prev_sar=prev_sar,
            direction=direction,
        )
    if kind == "ichimoku_trend":
        tenkan = _combo_value(indicators, rule, "ichimoku_tenkan", index)
        kijun = _combo_value(indicators, rule, "ichimoku_kijun", index)
        span_a = _combo_value(indicators, rule, "ichimoku_span_a", index)
        span_b = _combo_value(indicators, rule, "ichimoku_span_b", index)
        top = _combo_value(indicators, rule, "ichimoku_cloud_top", index)
        bottom = _combo_value(indicators, rule, "ichimoku_cloud_bottom", index)
        prev_tenkan = _combo_value(indicators, rule, "ichimoku_tenkan", index - 1) if index > 0 else float("nan")
        prev_kijun = _combo_value(indicators, rule, "ichimoku_kijun", index - 1) if index > 0 else float("nan")
        prev_close = float(rule_data.closes[max(0, rule_index - 1)]) if rule_index > 0 else float("nan")
        return _combo_ichimoku_condition_hit(
            str(params.get("buy_condition") or "strong_long_confirm"),
            close=close,
            prev_close=prev_close,
            tenkan=tenkan,
            prev_tenkan=prev_tenkan,
            kijun=kijun,
            prev_kijun=prev_kijun,
            span_a=span_a,
            span_b=span_b,
            cloud_top=top,
            cloud_bottom=bottom,
        )
    if kind == "ttm_squeeze":
        return _combo_ttm_condition_hit(str(params.get("buy_condition") or "long_release"), indicators, rule, index)
    if kind == "heikin_ashi_trend":
        direction = _combo_value(indicators, rule, "ha_direction", index)
        prev_direction = _combo_value(indicators, rule, "ha_direction", index - 1) if index > 0 else float("nan")
        body_pct = _combo_value(indicators, rule, "ha_body_pct", index)
        if is_valid(body_pct) and body_pct < float(params.get("doji_tolerance_pct") or 0.0):
            return False
        if str(params.get("mode") or "transition") == "state":
            return is_valid(direction) and direction > 0.0
        return is_valid(prev_direction) and is_valid(direction) and prev_direction <= 0.0 < direction
    return False


def _combo_rule_exit_signal(rule: dict[str, Any], data: OHLCVData, indicators: dict[str, Any], index: int) -> bool:
    kind = str(rule.get("kind") or "").lower()
    params = dict(rule.get("params") or {})
    rule_data, rule_index = _combo_data_and_index(data, indicators, rule, index)
    close = float(rule_data.closes[rule_index])
    if kind == "ma_cross":
        fast = _combo_value(indicators, rule, "fast_ma", index)
        slow = _combo_value(indicators, rule, "slow_ma", index)
        return (is_valid(fast) and close < fast) or (is_valid(fast) and is_valid(slow) and fast < slow)
    if kind == "rsi_momentum":
        rsi_now = _combo_value(indicators, rule, "rsi", index)
        return is_valid(rsi_now) and rsi_now <= float(params.get("exit_below") or 45)
    if kind == "rsi_derivative":
        drsi = _combo_value(indicators, rule, "rsi_derivative", index)
        return is_valid(drsi) and drsi <= float(params.get("sell_below") or 0.0)
    if kind == "macd_momentum":
        macd_now = _combo_value(indicators, rule, "macd", index)
        signal = _combo_value(indicators, rule, "macd_signal", index)
        hist = _combo_value(indicators, rule, "macd_hist", index)
        return (is_valid(macd_now) and is_valid(signal) and macd_now < signal) or (is_valid(hist) and hist < 0)
    if kind == "bollinger_pullback":
        percent_b = _combo_value(indicators, rule, "percent_b", index)
        return is_valid(percent_b) and percent_b >= float(params.get("exit_b") or 0.65)
    if kind == "bollinger_breakout":
        percent_b = _combo_value(indicators, rule, "percent_b", index)
        middle = _combo_value(indicators, rule, "bb_middle", index)
        return (is_valid(percent_b) and percent_b <= float(params.get("exit_b") or 0.55)) or (is_valid(middle) and close < middle)
    if kind == "donchian_breakout":
        low_channel = _combo_value(indicators, rule, "donchian_low", index)
        return is_valid(low_channel) and close < low_channel
    if kind == "supertrend_trend":
        trend = _combo_value(indicators, rule, "supertrend", index)
        direction = _combo_value(indicators, rule, "supertrend_direction", index)
        return (is_valid(trend) and close < trend) or (is_valid(direction) and direction < 0)
    if kind == "vwap_filter":
        vw = _combo_value(indicators, rule, "vwap", index)
        return is_valid(vw) and close < vw * (1.0 - float(params.get("exit_below_pct") or 0.012))
    if kind == "relative_volume":
        rv = _combo_value(indicators, rule, "relative_volume", index)
        prev_rv = _combo_value(indicators, rule, "relative_volume", index - 1) if index > 0 else float("nan")
        threshold = float(params.get("threshold") or 1.2)
        return is_valid(rv) and (rv <= threshold or (is_valid(prev_rv) and rv < prev_rv))
    if kind == "roc_momentum":
        roc = _combo_value(indicators, rule, "roc", index)
        prev_roc = _combo_value(indicators, rule, "roc", index - 1) if index > 0 else float("nan")
        return _combo_roc_condition_hit(
            str(params.get("sell_condition") or "momentum_short"),
            roc,
            prev_roc,
            float(params.get("sell_threshold_pct") or 0.0),
        )
    if kind == "sar_trend":
        sar = _combo_value(indicators, rule, "sar", index)
        prev_sar = _combo_value(indicators, rule, "sar", index - 1) if index > 0 else float("nan")
        prev_close = float(rule_data.closes[max(0, rule_index - 1)]) if rule_index > 0 else float("nan")
        direction = _combo_value(indicators, rule, "sar_direction", index)
        return _combo_sar_condition_hit(
            str(params.get("sell_condition") or "trend_short"),
            close=close,
            prev_close=prev_close,
            sar=sar,
            prev_sar=prev_sar,
            direction=direction,
        )
    if kind == "ichimoku_trend":
        tenkan = _combo_value(indicators, rule, "ichimoku_tenkan", index)
        kijun = _combo_value(indicators, rule, "ichimoku_kijun", index)
        span_a = _combo_value(indicators, rule, "ichimoku_span_a", index)
        span_b = _combo_value(indicators, rule, "ichimoku_span_b", index)
        top = _combo_value(indicators, rule, "ichimoku_cloud_top", index)
        bottom = _combo_value(indicators, rule, "ichimoku_cloud_bottom", index)
        prev_tenkan = _combo_value(indicators, rule, "ichimoku_tenkan", index - 1) if index > 0 else float("nan")
        prev_kijun = _combo_value(indicators, rule, "ichimoku_kijun", index - 1) if index > 0 else float("nan")
        prev_close = float(rule_data.closes[max(0, rule_index - 1)]) if rule_index > 0 else float("nan")
        return _combo_ichimoku_condition_hit(
            str(params.get("sell_condition") or "strong_short_confirm"),
            close=close,
            prev_close=prev_close,
            tenkan=tenkan,
            prev_tenkan=prev_tenkan,
            kijun=kijun,
            prev_kijun=prev_kijun,
            span_a=span_a,
            span_b=span_b,
            cloud_top=top,
            cloud_bottom=bottom,
        )
    if kind == "ttm_squeeze":
        return _combo_ttm_condition_hit(str(params.get("sell_condition") or "short_release"), indicators, rule, index)
    if kind == "heikin_ashi_trend":
        direction = _combo_value(indicators, rule, "ha_direction", index)
        prev_direction = _combo_value(indicators, rule, "ha_direction", index - 1) if index > 0 else float("nan")
        body_pct = _combo_value(indicators, rule, "ha_body_pct", index)
        if is_valid(body_pct) and body_pct < float(params.get("doji_tolerance_pct") or 0.0):
            return False
        if str(params.get("mode") or "transition") == "state":
            return is_valid(direction) and direction < 0.0
        return is_valid(prev_direction) and is_valid(direction) and prev_direction >= 0.0 > direction
    return False


def _combo_entry_signal(candidate: StrategyCandidate, data: OHLCVData, indicators: dict[str, Any], index: int) -> bool:
    rules = _combo_rules(candidate)
    if len(rules) < 2:
        return False
    threshold = max(2, min(len(rules), int(candidate.parameters.get("entry_threshold") or len(rules))))
    votes = sum(1 for rule in rules if _combo_rule_entry_signal(rule, data, indicators, index))
    return votes >= threshold


def _combo_exit_signal(candidate: StrategyCandidate, data: OHLCVData, indicators: dict[str, Any], index: int) -> bool:
    rules = _combo_rules(candidate)
    if not rules:
        return False
    threshold = max(1, min(len(rules), int(candidate.parameters.get("exit_threshold") or 1)))
    votes = sum(1 for rule in rules if _combo_rule_exit_signal(rule, data, indicators, index))
    return votes >= threshold


def entry_signal(candidate: StrategyCandidate, data: OHLCVData, indicators: dict[str, Any], index: int) -> bool:
    p = candidate.parameters
    close = float(data.closes[index])
    name = candidate.strategy_name
    if _is_combo_candidate(candidate):
        return _combo_entry_signal(candidate, data, indicators, index)
    if name == "ema_rsi_atr_trend":
        ma_fast = _value(indicators, "ma_fast", index)
        ma_slow = _value(indicators, "ma_slow", index)
        rsi_now = _value(indicators, "rsi", index)
        atr_d = _atr_derivative(indicators, index)
        return all(
            [
                is_valid(ma_fast),
                is_valid(ma_slow),
                is_valid(rsi_now),
                is_valid(atr_d),
                close > ma_fast,
                ma_fast > ma_slow,
                rsi_now > float(p["rsi_buy_min"]),
                atr_d >= 0,
            ]
        )
    if name == "ema_macd_atr_trend":
        ma_fast = _value(indicators, "ma_fast", index)
        ma_slow = _value(indicators, "ma_slow", index)
        macd_now = _value(indicators, "macd", index)
        macd_sig = _value(indicators, "macd_signal", index)
        hist = _value(indicators, "macd_hist", index)
        return all(
            [
                is_valid(ma_fast),
                is_valid(ma_slow),
                is_valid(macd_now),
                is_valid(macd_sig),
                close > ma_fast,
                ma_fast > ma_slow,
                macd_now > macd_sig,
                hist >= 0,
            ]
        )
    if name == "bollinger_trend_mean_reversion":
        trend = _value(indicators, "trend_ma", index)
        percent_b = _value(indicators, "percent_b", index)
        return is_valid(trend) and is_valid(percent_b) and close > trend and percent_b <= float(p["percent_b_buy"])
    if name == "vwap_pullback_volume":
        vw = _value(indicators, "vwap", index)
        rv = _value(indicators, "relative_volume", index)
        trend = _value(indicators, "trend_ma", index)
        distance = float(p["vwap_distance_pct"])
        return (
            is_valid(vw)
            and is_valid(rv)
            and is_valid(trend)
            and close >= trend
            and close <= vw * (1.0 + distance)
            and rv >= float(p["relative_volume_threshold"])
        )
    if name == "donchian_atr_breakout":
        high_channel = _value(indicators, "donchian_high", index)
        mode = str(p.get("breakout_confirmation") or "close_above_high")
        if not is_valid(high_channel):
            return False
        if mode == "high_above_high":
            return float(data.highs[index]) > high_channel
        return close > high_channel
    if name == "ichimoku_cloud_trend":
        tenkan = _value(indicators, "ichimoku_tenkan", index)
        kijun = _value(indicators, "ichimoku_kijun", index)
        top = _value(indicators, "ichimoku_cloud_top", index)
        ok = is_valid(tenkan) and is_valid(kijun) and tenkan > kijun
        if bool(p.get("cloud_confirmation")):
            ok = ok and is_valid(top) and close > top
        return bool(ok)
    if name == "supertrend_rsi_confirmation":
        st = _value(indicators, "supertrend", index)
        direction = _value(indicators, "supertrend_direction", index)
        rsi_now = _value(indicators, "rsi", index)
        return is_valid(st) and is_valid(direction) and is_valid(rsi_now) and close > st and direction > 0 and rsi_now > float(p["rsi_buy_min"])
    return False


def exit_signal(candidate: StrategyCandidate, data: OHLCVData, indicators: dict[str, Any], index: int) -> bool:
    p = candidate.parameters
    close = float(data.closes[index])
    name = candidate.strategy_name
    if _is_combo_candidate(candidate):
        return _combo_exit_signal(candidate, data, indicators, index)
    if name == "ema_rsi_atr_trend":
        ma_fast = _value(indicators, "ma_fast", index)
        rsi_now = _value(indicators, "rsi", index)
        return (is_valid(ma_fast) and close < ma_fast) or (is_valid(rsi_now) and rsi_now < float(p["rsi_exit"]))
    if name == "ema_macd_atr_trend":
        ma_fast = _value(indicators, "ma_fast", index)
        macd_now = _value(indicators, "macd", index)
        macd_sig = _value(indicators, "macd_signal", index)
        return (is_valid(ma_fast) and close < ma_fast) or (is_valid(macd_now) and is_valid(macd_sig) and macd_now < macd_sig)
    if name == "bollinger_trend_mean_reversion":
        mid = _value(indicators, "bb_middle", index)
        percent_b = _value(indicators, "percent_b", index)
        return (is_valid(mid) and close >= mid) or (is_valid(percent_b) and percent_b >= float(p["percent_b_exit"]))
    if name == "vwap_pullback_volume":
        vw = _value(indicators, "vwap", index)
        trend = _value(indicators, "trend_ma", index)
        distance = float(p["vwap_distance_pct"])
        return (is_valid(vw) and close > vw * (1.0 + distance)) or (is_valid(trend) and close < trend)
    if name == "donchian_atr_breakout":
        low_channel = _value(indicators, "donchian_low", index)
        return is_valid(low_channel) and close < low_channel
    if name == "ichimoku_cloud_trend":
        kijun = _value(indicators, "ichimoku_kijun", index)
        bottom = _value(indicators, "ichimoku_cloud_bottom", index)
        return (is_valid(kijun) and close < kijun) or (is_valid(bottom) and close < bottom)
    if name == "supertrend_rsi_confirmation":
        st = _value(indicators, "supertrend", index)
        direction = _value(indicators, "supertrend_direction", index)
        rsi_now = _value(indicators, "rsi", index)
        return (
            (is_valid(st) and close < st)
            or (is_valid(direction) and direction < 0)
            or (is_valid(rsi_now) and rsi_now < float(p["rsi_exit"]))
        )
    return False


def _fill_price(price: float, *, side: str, config: BacktestConfig) -> tuple[float, float]:
    slip = float(price) * (float(config.slippage_bps) / 10000.0)
    if side == "buy":
        return float(price) + slip, slip
    return max(0.01, float(price) - slip), slip


def _fee(notional: float, config: BacktestConfig) -> float:
    return abs(float(notional)) * float(config.commission_pct) + float(config.commission_flat)


def _regime_list(indicators: dict[str, Any], index: int) -> list[str]:
    regimes = indicators.get("regimes") or []
    try:
        return sorted(str(v) for v in regimes[index])
    except Exception:
        return []


def run_backtest(
    data: OHLCVData,
    candidate: StrategyCandidate,
    config: Optional[BacktestConfig] = None,
    *,
    data_by_timeframe: Optional[dict[str, OHLCVData]] = None,
) -> BacktestResult:
    if config is None:
        config = BacktestConfig()
    if len(data) < 5:
        metrics = {"ok": False, "reason": "insufficient candles", "trade_count": 0}
        return BacktestResult(candidate, data.symbol, data.timeframe, metrics, [], [])

    indicators = _compute_indicators(data, candidate, data_by_timeframe=data_by_timeframe)
    cash = float(config.initial_capital)
    position_qty = 0.0
    entry_price = 0.0
    entry_time = ""
    entry_index = 0
    entry_fees = 0.0
    entry_slippage = 0.0
    entry_regimes: list[str] = []
    active_stop: Optional[float] = None
    profit_target: Optional[float] = None
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    fees_paid = 0.0
    slippage_estimate = 0.0
    daily_trade_count: dict[str, int] = {}
    daily_realized_pnl: dict[str, float] = {}
    daily_start_equity: dict[str, float] = {}

    max_position_pct = min(float(config.max_position_pct), float(candidate.risk.get("max_position_pct", config.max_position_pct)))
    position_pct = min(float(config.default_position_pct), max_position_pct)
    max_daily_loss_pct = float(candidate.risk.get("max_daily_loss_pct", config.max_daily_loss_pct))
    max_trades_per_day = int(candidate.risk.get("max_trades_per_day", config.max_trades_per_day))

    def current_equity(mark_price: float) -> float:
        return cash + (position_qty * float(mark_price))

    def record_equity(index: int) -> None:
        equity_curve.append(
            {
                "time": data.timestamps[index],
                "equity": current_equity(data.closes[index]),
                "close": float(data.closes[index]),
            }
        )

    def close_position(exit_index: int, raw_price: float, reason: str) -> None:
        nonlocal cash, position_qty, entry_price, entry_time, entry_index, entry_fees, entry_slippage
        nonlocal fees_paid, slippage_estimate, active_stop, profit_target, entry_regimes
        if position_qty <= 0:
            return
        exit_price, slip = _fill_price(raw_price, side="sell", config=config)
        gross = (exit_price - entry_price) * position_qty
        exit_fee = _fee(exit_price * position_qty, config)
        fees = entry_fees + exit_fee
        net = gross - fees
        one_share_gross = exit_price - entry_price
        one_share_fees = _fee(entry_price, config) + _fee(exit_price, config)
        one_share_net = one_share_gross - one_share_fees
        cash += (exit_price * position_qty) - exit_fee
        fees_paid += exit_fee
        slippage_estimate += slip * position_qty
        day = _day_key(data.timestamps[exit_index])
        daily_realized_pnl[day] = daily_realized_pnl.get(day, 0.0) + net
        trades.append(
            {
                "symbol": data.symbol,
                "entry_time": entry_time,
                "exit_time": data.timestamps[exit_index],
                "side": "long",
                "entry_price": entry_price,
                "exit_price": exit_price,
                "quantity": position_qty,
                "gross_pnl": gross,
                "fees": fees,
                "slippage": entry_slippage + (slip * position_qty),
                "net_pnl": net,
                "one_share_gross_pnl": one_share_gross,
                "one_share_fees": one_share_fees,
                "one_share_net_pnl": one_share_net,
                "exit_reason": reason,
                "bars_held": max(1, exit_index - entry_index),
                "entry_regimes": entry_regimes,
            }
        )
        position_qty = 0.0
        entry_price = 0.0
        entry_time = ""
        entry_index = 0
        entry_fees = 0.0
        entry_slippage = 0.0
        active_stop = None
        profit_target = None
        entry_regimes = []

    for i in range(1, len(data) - 1):
        next_i = i + 1
        if i == 1:
            record_equity(i)
        day = _day_key(data.timestamps[next_i])
        if day not in daily_start_equity:
            daily_start_equity[day] = current_equity(data.closes[i])
            daily_trade_count[day] = 0
            daily_realized_pnl[day] = 0.0

        if position_qty > 0:
            atr_now = _value(indicators, "atr", i)
            if bool(candidate.parameters.get("atr_trailing", candidate.risk.get("atr_trailing_stop", True))) and is_valid(atr_now):
                trail = float(data.closes[i]) - (float(atr_now) * float(candidate.parameters.get("atr_stop_mult", 2.5)))
                active_stop = max(active_stop, trail) if active_stop is not None else trail

            exit_reason = ""
            raw_exit_price = 0.0
            next_open = float(data.opens[next_i]) if config.use_next_open else float(data.closes[next_i])
            if active_stop is not None and float(data.lows[next_i]) <= active_stop:
                raw_exit_price = next_open if next_open < active_stop else active_stop
                exit_reason = "atr_trailing_stop_hit"
            elif profit_target is not None and float(data.highs[next_i]) >= profit_target:
                raw_exit_price = next_open if next_open > profit_target else profit_target
                exit_reason = "profit_target"
            elif exit_signal(candidate, data, indicators, i):
                raw_exit_price = next_open
                exit_reason = "exit_rule"

            day_start = daily_start_equity.get(day, current_equity(data.closes[i]))
            if daily_realized_pnl.get(day, 0.0) <= -abs(max_daily_loss_pct) * day_start and not exit_reason:
                raw_exit_price = next_open
                exit_reason = "max_daily_loss"

            if exit_reason:
                close_position(next_i, raw_exit_price, exit_reason)

        if position_qty <= 0:
            day_start = daily_start_equity.get(day, current_equity(data.closes[i]))
            daily_loss_block = daily_realized_pnl.get(day, 0.0) <= -abs(max_daily_loss_pct) * day_start
            trade_count_block = daily_trade_count.get(day, 0) >= max_trades_per_day
            session_block = config.regular_hours_only and not _is_regular_session(data.sessions[next_i])
            if not daily_loss_block and not trade_count_block and not session_block and entry_signal(candidate, data, indicators, i):
                raw_entry = float(data.opens[next_i]) if config.use_next_open else float(data.closes[next_i])
                fill, slip = _fill_price(raw_entry, side="buy", config=config)
                equity = current_equity(data.closes[i])
                notional = max(0.0, equity * position_pct)
                qty = notional / fill if fill > 0 else 0.0
                if qty > 0:
                    fee = _fee(qty * fill, config)
                    total_cost = (qty * fill) + fee
                    if total_cost <= cash:
                        cash -= total_cost
                        position_qty = qty
                        entry_price = fill
                        entry_time = data.timestamps[next_i]
                        entry_index = next_i
                        entry_fees = fee
                        entry_slippage = slip * qty
                        entry_regimes = _regime_list(indicators, i)
                        fees_paid += fee
                        slippage_estimate += slip * qty
                        daily_trade_count[day] = daily_trade_count.get(day, 0) + 1
                        atr_now = _value(indicators, "atr", i)
                        if is_valid(atr_now):
                            active_stop = fill - (float(atr_now) * float(candidate.parameters.get("atr_stop_mult", 2.5)))
                            target_mult = candidate.risk.get("profit_target_atr_mult")
                            if target_mult is not None:
                                profit_target = fill + (float(atr_now) * float(target_mult))

        record_equity(next_i)

    if position_qty > 0 and config.close_open_position:
        close_position(len(data) - 1, float(data.closes[-1]), "end_of_data")
        if equity_curve:
            equity_curve[-1]["equity"] = current_equity(data.closes[-1])

    metrics = calculate_metrics(
        data=data,
        candidate=candidate,
        config=config,
        trades=trades,
        equity_curve=equity_curve,
        fees_paid=fees_paid,
        slippage_estimate=slippage_estimate,
    )
    monthly = monthly_return_distribution(equity_curve)
    metrics["monthly_return_distribution"] = monthly
    metrics["largest_month_profit_contribution"] = largest_month_profit_contribution(monthly)
    regimes = summarize_regimes(trades)
    return BacktestResult(candidate, data.symbol, data.timeframe, metrics, trades, equity_curve, monthly, regimes)


def calculate_metrics(
    *,
    data: OHLCVData,
    candidate: StrategyCandidate,
    config: BacktestConfig,
    trades: list[dict[str, Any]],
    equity_curve: list[dict[str, Any]],
    fees_paid: float,
    slippage_estimate: float,
) -> dict[str, Any]:
    equity_values = [float(row["equity"]) for row in equity_curve] or [float(config.initial_capital)]
    final_equity = equity_values[-1]
    total_return = (final_equity / float(config.initial_capital)) - 1.0
    gross_pnls = [float(t["gross_pnl"]) for t in trades]
    net_pnls = [float(t["net_pnl"]) for t in trades]
    one_share_gross_pnls = [
        float(t.get("one_share_gross_pnl", float(t.get("exit_price") or 0.0) - float(t.get("entry_price") or 0.0)))
        for t in trades
    ]
    one_share_net_pnls = [
        float(t.get("one_share_net_pnl", gross))
        for t, gross in zip(trades, one_share_gross_pnls)
    ]
    one_share_buy_notional = sum(float(t.get("entry_price") or 0.0) for t in trades)
    one_share_sell_notional = sum(float(t.get("exit_price") or 0.0) for t in trades)
    one_share_fees = sum(float(t.get("one_share_fees") or 0.0) for t in trades)
    wins = [p for p in net_pnls if p > 0]
    losses = [p for p in net_pnls if p < 0]
    trade_count = len(trades)
    returns = _pct_returns(equity_values)
    first_ts = parse_timestamp(data.timestamps[0]) if data.timestamps else None
    last_ts = parse_timestamp(data.timestamps[-1]) if data.timestamps else None
    years = 0.0
    if first_ts and last_ts and last_ts > first_ts:
        years = max((last_ts - first_ts).total_seconds() / (365.25 * 24 * 3600), 1.0 / 365.25)
    cagr = ((1.0 + total_return) ** (1.0 / years) - 1.0) if years > 0 and total_return > -1 else total_return
    total_profit = sum(wins)
    total_loss = abs(sum(losses))
    largest_win = max(wins) if wins else 0.0
    largest_loss = min(losses) if losses else 0.0
    avg_duration = mean([float(t.get("bars_held") or 0) for t in trades]) if trades else 0.0
    avg_trade_return = mean([p / float(config.initial_capital) for p in net_pnls]) if net_pnls else 0.0
    gross_return = sum(gross_pnls) / float(config.initial_capital)
    net_profit = final_equity - float(config.initial_capital)
    metrics = {
        "ok": True,
        "strategy_template": candidate.strategy_name,
        "symbol": data.symbol,
        "timeframe": data.timeframe,
        "start_date": data.timestamps[0] if data.timestamps else "",
        "end_date": data.timestamps[-1] if data.timestamps else "",
        "initial_capital": float(config.initial_capital),
        "final_equity": final_equity,
        "net_profit": net_profit,
        "one_share_gross_profit": sum(one_share_gross_pnls),
        "one_share_net_profit": sum(one_share_net_pnls),
        "one_share_buy_notional": one_share_buy_notional,
        "one_share_sell_notional": one_share_sell_notional,
        "one_share_fees": one_share_fees,
        "one_share_return": _safe_ratio(sum(one_share_net_pnls), one_share_buy_notional),
        "gross_return": gross_return,
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe_ratio(returns, data.timeframe),
        "sortino": sortino_ratio(returns, data.timeframe),
        "max_drawdown": max_drawdown(equity_values),
        "win_rate": _safe_ratio(len(wins), trade_count),
        "profit_factor": profit_factor(net_pnls),
        "expectancy": mean(net_pnls) if net_pnls else 0.0,
        "average_win": mean(wins) if wins else 0.0,
        "average_loss": mean(losses) if losses else 0.0,
        "average_trade_duration": avg_duration,
        "trade_count": trade_count,
        "fees_paid": float(fees_paid),
        "slippage_estimate": float(slippage_estimate),
        "largest_winning_trade_contribution": _safe_ratio(largest_win, total_profit),
        "largest_losing_trade_contribution": _safe_ratio(abs(largest_loss), total_loss),
        "avg_trade_return": avg_trade_return,
        "bars": len(data),
        "turnover": _safe_ratio(trade_count, max(1, len(data))),
    }
    if _is_combo_candidate(candidate):
        metrics["rule_timeframes"] = _combo_rule_timeframes(candidate, data.timeframe)
    return metrics


def monthly_return_distribution(equity_curve: list[dict[str, Any]]) -> dict[str, float]:
    if not equity_curve:
        return {}
    buckets: dict[str, list[float]] = {}
    for row in equity_curve:
        buckets.setdefault(_month_key(str(row.get("time") or "")), []).append(float(row.get("equity") or 0.0))
    out: dict[str, float] = {}
    for month, values in buckets.items():
        if values and values[0] > 0:
            out[month] = (values[-1] / values[0]) - 1.0
    return out


def largest_month_profit_contribution(monthly_returns: dict[str, float]) -> float:
    profits = [float(v) for v in monthly_returns.values() if float(v) > 0]
    total = sum(profits)
    return max(profits) / total if total > 0 else 0.0


def summarize_regimes(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for trade in trades:
        pnl = float(trade.get("net_pnl") or 0.0)
        regimes = trade.get("entry_regimes") or ["unlabeled"]
        for regime in regimes:
            row = summary.setdefault(str(regime), {"trade_count": 0, "net_pnl": 0.0, "wins": 0})
            row["trade_count"] += 1
            row["net_pnl"] += pnl
            if pnl > 0:
                row["wins"] += 1
    for row in summary.values():
        row["win_rate"] = _safe_ratio(int(row["wins"]), int(row["trade_count"]))
    return summary

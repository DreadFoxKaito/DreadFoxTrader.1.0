from __future__ import annotations

import copy
import json
import random
from dataclasses import dataclass
from statistics import mean
from typing import Any, Iterable, Optional

from .strategy_templates import StrategyCandidate

COMBO_STRATEGY_NAME = "indicator_combo_evolved"
COMBO_STRATEGY_KIND = "open_indicator_combo"
COMBO_TIMEFRAMES: tuple[str, ...] = ("1m", "5m", "10m", "15m", "30m", "1h", "1d")

RULE_KINDS: tuple[str, ...] = (
    "ma_cross",
    "rsi_momentum",
    "rsi_derivative",
    "macd_momentum",
    "bollinger_pullback",
    "bollinger_breakout",
    "donchian_breakout",
    "supertrend_trend",
    "vwap_filter",
    "relative_volume",
    "roc_momentum",
    "sar_trend",
    "ichimoku_trend",
    "ttm_squeeze",
    "heikin_ashi_trend",
)

ROC_BUY_CONDITIONS: tuple[str, ...] = (
    "momentum_long",
    "roc_cross_up_zero",
    "roc_above_threshold",
    "roc_cross_up_threshold",
    "roc_increasing",
    "roc_positive",
)
ROC_SELL_CONDITIONS: tuple[str, ...] = (
    "momentum_short",
    "roc_cross_down_zero",
    "roc_below_threshold",
    "roc_cross_down_threshold",
    "roc_decreasing",
    "roc_negative",
)
SAR_BUY_CONDITIONS: tuple[str, ...] = (
    "sar_cross_up",
    "price_above_sar",
    "sar_rising",
    "trend_long",
)
SAR_SELL_CONDITIONS: tuple[str, ...] = (
    "sar_cross_down",
    "price_below_sar",
    "sar_falling",
    "trend_short",
)
ICHI_BUY_CONDITIONS: tuple[str, ...] = (
    "strong_long_confirm",
    "full_bullish_stack",
    "partial_bullish_stack",
    "price_above_cloud",
    "cloud_bullish",
    "tenkan_cross_above",
    "tenkan_above_kijun",
)
ICHI_SELL_CONDITIONS: tuple[str, ...] = (
    "strong_short_confirm",
    "full_bearish_stack",
    "partial_bearish_stack",
    "price_below_cloud",
    "cloud_bearish",
    "tenkan_cross_below",
    "tenkan_below_kijun",
)
TTM_BUY_CONDITIONS: tuple[str, ...] = (
    "long_release",
    "long_trend",
    "squeeze_fired",
    "momentum_above_zero",
    "momentum_increasing",
    "momentum_cross_up",
)
TTM_SELL_CONDITIONS: tuple[str, ...] = (
    "short_release",
    "short_trend",
    "momentum_below_zero",
    "momentum_decreasing",
    "momentum_cross_down",
)


def _clamp(value: float, low: float, high: float) -> float:
    return max(float(low), min(float(high), float(value)))


def _clamp_int(value: Any, low: int, high: int) -> int:
    try:
        numeric = int(round(float(value)))
    except Exception:
        numeric = int(low)
    return int(max(int(low), min(int(high), numeric)))


def _clamp_float(value: Any, low: float, high: float, *, digits: int = 4) -> float:
    try:
        numeric = float(value)
    except Exception:
        numeric = float(low)
    return round(_clamp(numeric, low, high), digits)


def _choice(value: Any, choices: Iterable[Any], fallback: Any) -> Any:
    allowed = tuple(choices)
    return value if value in allowed else fallback


def normalize_symbols(symbols: Iterable[Any]) -> list[str]:
    out: list[str] = []
    for symbol in symbols or []:
        txt = str(symbol or "").strip().upper()
        if txt and txt not in out:
            out.append(txt)
    return out


def normalize_timeframe(value: Any, *, default: str = "1h") -> str:
    txt = str(value or "").strip().lower()
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
    txt = aliases.get(txt, txt)
    if txt in COMBO_TIMEFRAMES:
        return txt
    fallback = aliases.get(str(default or "").strip().lower(), str(default or "").strip().lower())
    return fallback if fallback in COMBO_TIMEFRAMES else "1h"


def _rule_timeframe(rule: dict[str, Any], *, default: str = "1h") -> str:
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    return normalize_timeframe(rule.get("timeframe") or params.get("timeframe"), default=default)


def candidate_rule_timeframes(candidate: StrategyCandidate) -> list[str]:
    default = normalize_timeframe(candidate.timeframe or "1h")
    out: list[str] = []
    for rule in candidate.parameters.get("rules") or []:
        if not isinstance(rule, dict):
            continue
        tf = _rule_timeframe(rule, default=default)
        if tf not in out:
            out.append(tf)
    return out or [default]


def normalize_rule(rule: dict[str, Any], *, default_timeframe: str = "1h") -> dict[str, Any]:
    kind = str(rule.get("kind") or "").strip().lower()
    params = dict(rule.get("params") or {})
    timeframe = _rule_timeframe(rule, default=default_timeframe)
    params.pop("timeframe", None)
    if kind not in RULE_KINDS:
        kind = "ma_cross"

    if kind == "ma_cross":
        fast = _clamp_int(params.get("fast", 12), 3, 90)
        slow = _clamp_int(params.get("slow", max(20, fast + 5)), max(fast + 2, 5), 260)
        params = {
            "ma_type": str(_choice(str(params.get("ma_type") or "ema").lower(), ("ema", "sma"), "ema")),
            "fast": fast,
            "slow": slow,
        }
    elif kind == "rsi_momentum":
        entry_min = _clamp_int(params.get("entry_min", 55), 35, 75)
        exit_below = _clamp_int(params.get("exit_below", min(45, entry_min - 5)), 20, max(21, entry_min - 1))
        params = {
            "length": _clamp_int(params.get("length", 14), 3, 40),
            "entry_min": entry_min,
            "exit_below": exit_below,
        }
    elif kind == "rsi_derivative":
        params = {
            "length": _clamp_int(params.get("length", 14), 3, 40),
            "buy_above": _clamp_float(params.get("buy_above", 0.0), -10.0, 10.0),
            "sell_below": _clamp_float(params.get("sell_below", 0.0), -10.0, 10.0),
        }
    elif kind == "macd_momentum":
        fast = _clamp_int(params.get("fast", 12), 3, 24)
        slow = _clamp_int(params.get("slow", max(26, fast + 5)), max(fast + 2, 6), 60)
        params = {
            "fast": fast,
            "slow": slow,
            "signal": _clamp_int(params.get("signal", 9), 3, 24),
            "hist_min": _clamp_float(params.get("hist_min", 0.0), -1.0, 1.0),
        }
    elif kind == "bollinger_pullback":
        entry_b = _clamp_float(params.get("entry_b", 0.25), -0.2, 0.55)
        exit_b = _clamp_float(params.get("exit_b", max(0.6, entry_b + 0.25)), max(-0.1, entry_b + 0.05), 1.2)
        params = {
            "length": _clamp_int(params.get("length", 20), 5, 100),
            "std_mult": _clamp_float(params.get("std_mult", 2.0), 1.0, 4.0),
            "entry_b": entry_b,
            "exit_b": exit_b,
        }
    elif kind == "bollinger_breakout":
        entry_b = _clamp_float(params.get("entry_b", 0.85), 0.55, 1.4)
        exit_b = _clamp_float(params.get("exit_b", min(0.55, entry_b - 0.2)), -0.1, max(-0.05, entry_b - 0.05))
        params = {
            "length": _clamp_int(params.get("length", 20), 5, 100),
            "std_mult": _clamp_float(params.get("std_mult", 2.0), 1.0, 4.0),
            "entry_b": entry_b,
            "exit_b": exit_b,
        }
    elif kind == "donchian_breakout":
        params = {
            "lookback": _clamp_int(params.get("lookback", 20), 5, 220),
            "use_high_break": bool(params.get("use_high_break", False)),
        }
    elif kind == "supertrend_trend":
        params = {
            "atr_length": _clamp_int(params.get("atr_length", 10), 3, 40),
            "multiplier": _clamp_float(params.get("multiplier", 3.0), 1.0, 6.0),
        }
    elif kind == "vwap_filter":
        params = {
            "max_extension_pct": _clamp_float(params.get("max_extension_pct", 0.015), 0.0, 0.08, digits=5),
            "max_pullback_pct": _clamp_float(params.get("max_pullback_pct", 0.01), 0.0, 0.08, digits=5),
            "exit_below_pct": _clamp_float(params.get("exit_below_pct", 0.012), 0.0, 0.08, digits=5),
        }
    elif kind == "relative_volume":
        params = {
            "length": _clamp_int(params.get("length", 20), 5, 80),
            "threshold": _clamp_float(params.get("threshold", 1.2), 0.5, 4.0),
        }
    elif kind == "roc_momentum":
        params = {
            "length": _clamp_int(params.get("length", 12), 2, 60),
            "buy_condition": str(_choice(str(params.get("buy_condition") or "momentum_long").lower(), ROC_BUY_CONDITIONS, "momentum_long")),
            "sell_condition": str(_choice(str(params.get("sell_condition") or "momentum_short").lower(), ROC_SELL_CONDITIONS, "momentum_short")),
            "buy_threshold_pct": _clamp_float(params.get("buy_threshold_pct", 0.0), -25.0, 25.0),
            "sell_threshold_pct": _clamp_float(params.get("sell_threshold_pct", 0.0), -25.0, 25.0),
        }
    elif kind == "sar_trend":
        step = _clamp_float(params.get("step", 0.02), 0.001, 0.10, digits=5)
        params = {
            "step": step,
            "max_step": _clamp_float(params.get("max_step", 0.2), step, 0.50, digits=5),
            "buy_condition": str(_choice(str(params.get("buy_condition") or "trend_long").lower(), SAR_BUY_CONDITIONS, "trend_long")),
            "sell_condition": str(_choice(str(params.get("sell_condition") or "trend_short").lower(), SAR_SELL_CONDITIONS, "trend_short")),
        }
    elif kind == "ichimoku_trend":
        tenkan = _clamp_int(params.get("tenkan", params.get("tenkan_length", 9)), 3, 30)
        kijun = _clamp_int(params.get("kijun", params.get("kijun_length", max(26, tenkan + 5))), max(tenkan + 2, 5), 80)
        senkou_b = _clamp_int(params.get("senkou_b", params.get("senkou_b_length", max(52, kijun + 10))), max(kijun + 2, 10), 160)
        params = {
            "tenkan": tenkan,
            "kijun": kijun,
            "senkou_b": senkou_b,
            "buy_condition": str(_choice(str(params.get("buy_condition") or "strong_long_confirm").lower(), ICHI_BUY_CONDITIONS, "strong_long_confirm")),
            "sell_condition": str(_choice(str(params.get("sell_condition") or "strong_short_confirm").lower(), ICHI_SELL_CONDITIONS, "strong_short_confirm")),
        }
    elif kind == "ttm_squeeze":
        params = {
            "bb_length": _clamp_int(params.get("bb_length", 20), 5, 80),
            "bb_mult": _clamp_float(params.get("bb_mult", 2.0), 0.5, 4.0),
            "kc_length": _clamp_int(params.get("kc_length", 20), 5, 80),
            "kc_mult": _clamp_float(params.get("kc_mult", 1.5), 0.5, 4.0),
            "momentum_length": _clamp_int(params.get("momentum_length", 20), 3, 80),
            "buy_condition": str(_choice(str(params.get("buy_condition") or "long_release").lower(), TTM_BUY_CONDITIONS, "long_release")),
            "sell_condition": str(_choice(str(params.get("sell_condition") or "short_release").lower(), TTM_SELL_CONDITIONS, "short_release")),
        }
    elif kind == "heikin_ashi_trend":
        mode = str(params.get("mode") or "transition").strip().lower()
        params = {
            "mode": mode if mode in {"transition", "state"} else "transition",
            "doji_tolerance_pct": _clamp_float(params.get("doji_tolerance_pct", 0.0), 0.0, 1.0),
        }

    return {"kind": kind, "timeframe": timeframe, "params": params}


def _rule_label(rule: dict[str, Any]) -> str:
    kind = str(rule.get("kind") or "")
    p = dict(rule.get("params") or {})
    tf = _rule_timeframe(rule)
    prefix = f"{tf} "
    if kind == "ma_cross":
        return f"{prefix}{str(p.get('ma_type') or 'ema').upper()} {p.get('fast')}/{p.get('slow')} trend"
    if kind == "rsi_momentum":
        return f"{prefix}RSI {p.get('length')} >= {p.get('entry_min')}"
    if kind == "rsi_derivative":
        return f"{prefix}dRSI {p.get('length')} >= {p.get('buy_above')}"
    if kind == "macd_momentum":
        return f"{prefix}MACD {p.get('fast')}/{p.get('slow')}/{p.get('signal')}"
    if kind == "bollinger_pullback":
        return f"{prefix}Bollinger pullback {p.get('length')} <= {p.get('entry_b')}"
    if kind == "bollinger_breakout":
        return f"{prefix}Bollinger breakout {p.get('length')} >= {p.get('entry_b')}"
    if kind == "donchian_breakout":
        return f"{prefix}Donchian breakout {p.get('lookback')}"
    if kind == "supertrend_trend":
        return f"{prefix}Supertrend {p.get('atr_length')} x {p.get('multiplier')}"
    if kind == "vwap_filter":
        return f"{prefix}VWAP location filter"
    if kind == "relative_volume":
        return f"{prefix}Relative volume >= {p.get('threshold')}"
    if kind == "roc_momentum":
        return f"{prefix}ROC {p.get('length')} {p.get('buy_condition')}"
    if kind == "sar_trend":
        return f"{prefix}Parabolic SAR {p.get('buy_condition')}"
    if kind == "ichimoku_trend":
        return f"{prefix}Ichimoku {p.get('tenkan')}/{p.get('kijun')}/{p.get('senkou_b')}"
    if kind == "ttm_squeeze":
        return f"{prefix}TTM {p.get('buy_condition')}"
    if kind == "heikin_ashi_trend":
        return f"{prefix}Heikin Ashi {p.get('mode')}"
    return f"{prefix}{kind}"


def build_combo_candidate(
    *,
    symbols: list[str],
    timeframe: str,
    rules: list[dict[str, Any]],
    entry_threshold: Optional[int] = None,
    exit_threshold: Optional[int] = None,
    atr_length: int = 14,
    atr_stop_mult: float = 2.5,
    atr_trailing: bool = True,
    profit_target_atr_mult: Optional[float] = None,
    max_position_pct: float = 0.15,
    max_daily_loss_pct: float = 0.03,
    max_trades_per_day: int = 8,
) -> StrategyCandidate:
    normalized: list[dict[str, Any]] = []
    default_tf = normalize_timeframe(timeframe)
    for raw in rules:
        if not isinstance(raw, dict):
            continue
        rule = normalize_rule(raw, default_timeframe=default_tf)
        rule["id"] = f"{rule['kind']}_{len(normalized) + 1}"
        normalized.append(rule)
    if len(normalized) < 2:
        raise ValueError("open combo strategies require at least two indicator rules")

    rule_count = len(normalized)
    entry_count = _clamp_int(entry_threshold if entry_threshold is not None else rule_count, 2, rule_count)
    exit_count = _clamp_int(exit_threshold if exit_threshold is not None else 1, 1, rule_count)
    target = None
    if profit_target_atr_mult is not None:
        target = _clamp_float(profit_target_atr_mult, 0.5, 8.0)

    params: dict[str, Any] = {
        "strategy_kind": COMBO_STRATEGY_KIND,
        "rules": normalized,
        "entry_threshold": entry_count,
        "exit_threshold": exit_count,
        "atr_length": _clamp_int(atr_length, 3, 60),
        "atr_stop_mult": _clamp_float(atr_stop_mult, 0.5, 8.0),
        "atr_trailing": bool(atr_trailing),
        "rule_timeframes": sorted({_rule_timeframe(rule, default=default_tf) for rule in normalized}),
    }
    risk = {
        "max_position_pct": _clamp_float(max_position_pct, 0.01, 1.0),
        "max_daily_loss_pct": _clamp_float(max_daily_loss_pct, 0.001, 0.50),
        "max_trades_per_day": _clamp_int(max_trades_per_day, 1, 100),
        "atr_trailing_stop": bool(atr_trailing),
        "profit_target_atr_mult": target,
    }
    labels = [_rule_label(rule) for rule in normalized]
    return StrategyCandidate(
        strategy_name=COMBO_STRATEGY_NAME,
        timeframe=default_tf,
        symbols=normalize_symbols(symbols),
        parameters=params,
        entry_rule={"at_least": [f"{entry_count} of {rule_count} indicator confirmations"], "rules": labels},
        exit_rule={"at_least": [f"{exit_count} of {rule_count} exit conditions"], "rules": labels},
        risk=risk,
    )


def candidate_signature(candidate: StrategyCandidate) -> str:
    p = candidate.parameters
    payload = {
        "timeframe": candidate.timeframe,
        "symbols": sorted(normalize_symbols(candidate.symbols)),
        "rules": p.get("rules") or [],
        "entry_threshold": p.get("entry_threshold"),
        "exit_threshold": p.get("exit_threshold"),
        "atr_length": p.get("atr_length"),
        "atr_stop_mult": p.get("atr_stop_mult"),
        "atr_trailing": p.get("atr_trailing"),
        "risk": candidate.risk,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def describe_candidate(candidate: StrategyCandidate, *, max_rules: int = 5) -> str:
    p = candidate.parameters
    rules = [str(_rule_label(rule)) for rule in list(p.get("rules") or [])[: int(max_rules)]]
    if len(p.get("rules") or []) > int(max_rules):
        rules.append("...")
    threshold = p.get("entry_threshold")
    symbol_text = ",".join(normalize_symbols(candidate.symbols))
    prefix = f"{symbol_text} | " if symbol_text else ""
    return f"{prefix}{threshold}/{len(p.get('rules') or [])}: " + "; ".join(rules)


def combo_search_score(metrics: dict[str, Any], *, min_trades: int = 1) -> float:
    trade_count = int(metrics.get("trade_count") or 0)
    if trade_count <= 0:
        return -1.0
    total_return = float(metrics.get("total_return") or 0.0)
    worst_return = float(metrics.get("worst_symbol_return", total_return) or 0.0)
    max_drawdown = float(metrics.get("max_drawdown") or 0.0)
    sortino = max(-5.0, min(5.0, float(metrics.get("sortino") or 0.0)))
    profit_factor = max(0.0, min(5.0, float(metrics.get("profit_factor") or 0.0)))
    win_rate = max(0.0, min(1.0, float(metrics.get("win_rate") or 0.0)))
    profit_factor_quality = max(-0.50, min(1.0, (profit_factor - 1.0) / 3.0))
    win_rate_quality = max(-1.0, min(1.0, (win_rate - 0.50) / 0.35))
    trade_factor = min(1.0, trade_count / float(max(1, int(min_trades))))
    raw = (
        (0.60 * total_return)
        + (0.25 * worst_return)
        + (0.035 * sortino)
        + (0.180 * profit_factor_quality)
        + (0.160 * win_rate_quality)
        - (0.350 * max_drawdown)
    )
    return raw * (0.25 + (0.75 * trade_factor))


def aggregate_result_metrics(results: list[Any]) -> dict[str, Any]:
    if not results:
        return {
            "ok": False,
            "reason": "no symbol results",
            "trade_count": 0,
            "total_return": 0.0,
        }
    metrics = [dict(item.metrics) for item in results]
    result_symbols = [str(item.symbol).upper() for item in results]
    rule_timeframes: list[str] = []
    for row in metrics:
        for tf in row.get("rule_timeframes") or []:
            tf = str(tf or "").strip().lower()
            if tf and tf not in rule_timeframes:
                rule_timeframes.append(tf)
    returns = [float(row.get("total_return") or 0.0) for row in metrics]
    drawdowns = [float(row.get("max_drawdown") or 0.0) for row in metrics]
    trade_counts = [int(row.get("trade_count") or 0) for row in metrics]
    total_trades = sum(trade_counts)
    one_share_net_profit = sum(float(row.get("one_share_net_profit") or 0.0) for row in metrics)
    one_share_gross_profit = sum(float(row.get("one_share_gross_profit") or 0.0) for row in metrics)
    one_share_buy_notional = sum(float(row.get("one_share_buy_notional") or 0.0) for row in metrics)
    one_share_sell_notional = sum(float(row.get("one_share_sell_notional") or 0.0) for row in metrics)
    one_share_fees = sum(float(row.get("one_share_fees") or 0.0) for row in metrics)

    def weighted(name: str) -> float:
        if total_trades > 0:
            return sum(float(row.get(name) or 0.0) * trades for row, trades in zip(metrics, trade_counts)) / float(total_trades)
        return mean([float(row.get(name) or 0.0) for row in metrics])

    first = metrics[0]
    return {
        "ok": True,
        "strategy_template": COMBO_STRATEGY_NAME,
        "symbol": ",".join(result_symbols),
        "tested_symbols": result_symbols,
        "symbol_count": len(result_symbols),
        "timeframe": str(results[0].timeframe),
        "rule_timeframes": rule_timeframes,
        "start_date": str(first.get("start_date") or ""),
        "end_date": str(first.get("end_date") or ""),
        "initial_capital": float(first.get("initial_capital") or 0.0),
        "final_equity": mean([float(row.get("final_equity") or 0.0) for row in metrics]),
        "net_profit": mean([float(row.get("net_profit") or 0.0) for row in metrics]),
        "one_share_gross_profit": one_share_gross_profit,
        "one_share_net_profit": one_share_net_profit,
        "one_share_buy_notional": one_share_buy_notional,
        "one_share_sell_notional": one_share_sell_notional,
        "one_share_fees": one_share_fees,
        "one_share_return": one_share_net_profit / one_share_buy_notional if one_share_buy_notional > 0 else 0.0,
        "gross_return": mean([float(row.get("gross_return") or 0.0) for row in metrics]),
        "total_return": mean(returns),
        "best_symbol_return": max(returns),
        "worst_symbol_return": min(returns),
        "cagr": mean([float(row.get("cagr") or 0.0) for row in metrics]),
        "sharpe": mean([float(row.get("sharpe") or 0.0) for row in metrics]),
        "sortino": mean([float(row.get("sortino") or 0.0) for row in metrics]),
        "max_drawdown": max(drawdowns),
        "win_rate": weighted("win_rate"),
        "profit_factor": mean([min(999.0, float(row.get("profit_factor") or 0.0)) for row in metrics]),
        "expectancy": weighted("expectancy"),
        "average_trade_duration": weighted("average_trade_duration"),
        "trade_count": total_trades,
        "fees_paid": sum(float(row.get("fees_paid") or 0.0) for row in metrics),
        "slippage_estimate": sum(float(row.get("slippage_estimate") or 0.0) for row in metrics),
        "avg_trade_return": weighted("avg_trade_return"),
        "bars": sum(int(row.get("bars") or 0) for row in metrics),
        "turnover": mean([float(row.get("turnover") or 0.0) for row in metrics]),
        "symbol_returns": {str(item.symbol): float(item.metrics.get("total_return") or 0.0) for item in results},
        "symbol_one_share_net_profit": {str(item.symbol): float(item.metrics.get("one_share_net_profit") or 0.0) for item in results},
        "symbol_trade_counts": {str(item.symbol): int(item.metrics.get("trade_count") or 0) for item in results},
    }


def grade_combo_metrics(metrics: dict[str, Any], *, min_trades: int) -> tuple[str, list[str], float]:
    reasons: list[str] = []
    total_return = float(metrics.get("total_return") or 0.0)
    worst_return = float(metrics.get("worst_symbol_return", total_return) or 0.0)
    drawdown = float(metrics.get("max_drawdown") or 0.0)
    trade_count = int(metrics.get("trade_count") or 0)
    if trade_count < int(min_trades):
        reasons.append("trade_count_below_minimum")
    if total_return <= 0:
        reasons.append("average_return_not_positive")
    if worst_return <= -0.05:
        reasons.append("one_or_more_symbols_underperformed")
    if drawdown > 0.30:
        reasons.append("max_drawdown_exceeds_limit")

    score = combo_search_score(metrics, min_trades=min_trades)
    if reasons:
        return "Reject", reasons, max(0.0, min(1.0, score + 0.5))
    if total_return >= 0.20 and score >= 0.15:
        return "A", reasons, 1.0
    if total_return >= 0.08 and score >= 0.06:
        return "B", reasons, 0.8
    return "C", reasons, 0.65


@dataclass
class OpenComboGenerator:
    seed: Optional[int] = None
    min_rules: int = 2
    max_rules: int = 5
    timeframes: tuple[str, ...] = COMBO_TIMEFRAMES
    universe_symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        self.random = random.Random(self.seed)
        self.min_rules = _clamp_int(self.min_rules, 2, 30)
        self.max_rules = _clamp_int(self.max_rules, self.min_rules, 30)
        normalized = []
        for item in self.timeframes or COMBO_TIMEFRAMES:
            tf = normalize_timeframe(item)
            if tf not in normalized:
                normalized.append(tf)
        self.timeframes = tuple(normalized or ("1h",))
        self.universe_symbols = tuple(normalize_symbols(self.universe_symbols))

    def _random_timeframe(self, default: str = "1h") -> str:
        if not self.timeframes:
            return normalize_timeframe(default)
        return str(self.random.choice(tuple(self.timeframes)))

    def _symbol_pool(self, symbols: Iterable[Any]) -> list[str]:
        pool = normalize_symbols(self.universe_symbols) or normalize_symbols(symbols)
        return pool or normalize_symbols(symbols)

    def _random_symbol_subset(self, symbols: Iterable[Any]) -> list[str]:
        pool = self._symbol_pool(symbols)
        if len(pool) <= 1:
            return pool
        if self.random.random() < 0.65:
            return pool
        count = self.random.randint(1, len(pool))
        selected = set(self.random.sample(pool, k=count))
        return [symbol for symbol in pool if symbol in selected]

    def _mutate_symbols(self, symbols: Iterable[Any]) -> list[str]:
        pool = self._symbol_pool(symbols)
        current = [symbol for symbol in normalize_symbols(symbols) if symbol in pool] or list(pool)
        if len(pool) <= 1:
            return current
        mutated = list(current)
        if len(mutated) > 1 and self.random.random() < 0.55:
            max_remove = min(len(mutated) - 1, 2)
            remove_count = self.random.randint(1, max_remove)
            for symbol in self.random.sample(mutated, k=remove_count):
                mutated.remove(symbol)
        available = [symbol for symbol in pool if symbol not in mutated]
        if available and self.random.random() < 0.25:
            mutated.append(self.random.choice(available))
        if not mutated:
            mutated.append(self.random.choice(pool))
        return [symbol for symbol in pool if symbol in set(mutated)]

    def _crossover_symbols(self, left: StrategyCandidate, right: StrategyCandidate) -> list[str]:
        left_symbols = normalize_symbols(left.symbols)
        right_symbols = normalize_symbols(right.symbols)
        pool = self._symbol_pool(left_symbols + right_symbols)
        if len(pool) <= 1:
            return pool
        overlap = [symbol for symbol in left_symbols if symbol in right_symbols]
        union = [symbol for symbol in pool if symbol in set(left_symbols + right_symbols)]
        mode = self.random.random()
        if overlap and mode < 0.30:
            selected = overlap
        elif mode < 0.65:
            selected = list(self.random.choice((left_symbols, right_symbols))) or union
        else:
            selected = union or pool
        if selected and self.random.random() < 0.35:
            selected = self._mutate_symbols(selected)
        return selected or [self.random.choice(pool)]

    def _maybe_mutate_rule_timeframe(self, rule: dict[str, Any]) -> dict[str, Any]:
        out = copy.deepcopy(rule)
        if len(self.timeframes) <= 1:
            out["timeframe"] = normalize_timeframe(out.get("timeframe"), default=self.timeframes[0] if self.timeframes else "1h")
            return out
        current = _rule_timeframe(out, default=self.timeframes[0])
        choices = [tf for tf in self.timeframes if tf != current] or list(self.timeframes)
        out["timeframe"] = str(self.random.choice(choices))
        return out

    def random_rule(self, kind: Optional[str] = None, *, timeframe: Optional[str] = None) -> dict[str, Any]:
        selected = str(kind or self.random.choice(RULE_KINDS))
        rule_timeframe = normalize_timeframe(timeframe, default=self._random_timeframe())
        if selected == "ma_cross":
            fast = self.random.randint(3, 60)
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "ma_type": self.random.choice(("ema", "sma")),
                        "fast": fast,
                        "slow": self.random.randint(max(fast + 5, 12), 260),
                    },
                }
            )
        if selected == "rsi_momentum":
            entry = self.random.randint(42, 70)
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "length": self.random.randint(4, 30),
                        "entry_min": entry,
                        "exit_below": self.random.randint(25, max(26, entry - 3)),
                    },
                }
            )
        if selected == "rsi_derivative":
            buy_above = round(self.random.uniform(-3.0, 4.0), 4)
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "length": self.random.randint(4, 30),
                        "buy_above": buy_above,
                        "sell_below": round(self.random.uniform(-4.0, min(3.0, buy_above)), 4),
                    },
                }
            )
        if selected == "macd_momentum":
            fast = self.random.randint(4, 18)
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "fast": fast,
                        "slow": self.random.randint(max(fast + 3, 12), 45),
                        "signal": self.random.randint(4, 16),
                        "hist_min": round(self.random.uniform(-0.2, 0.3), 4),
                    },
                }
            )
        if selected in {"bollinger_pullback", "bollinger_breakout"}:
            if selected == "bollinger_pullback":
                entry = round(self.random.uniform(0.05, 0.45), 4)
                exit_b = round(self.random.uniform(max(0.5, entry + 0.15), 1.0), 4)
            else:
                entry = round(self.random.uniform(0.70, 1.15), 4)
                exit_b = round(self.random.uniform(0.20, min(0.75, entry - 0.05)), 4)
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "length": self.random.randint(8, 80),
                        "std_mult": round(self.random.uniform(1.4, 3.2), 4),
                        "entry_b": entry,
                        "exit_b": exit_b,
                    },
                }
            )
        if selected == "donchian_breakout":
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "lookback": self.random.randint(8, 160),
                        "use_high_break": self.random.choice((True, False)),
                    },
                }
            )
        if selected == "supertrend_trend":
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "atr_length": self.random.randint(5, 30),
                        "multiplier": round(self.random.uniform(1.5, 5.0), 4),
                    },
                }
            )
        if selected == "vwap_filter":
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "max_extension_pct": round(self.random.uniform(0.002, 0.05), 5),
                        "max_pullback_pct": round(self.random.uniform(0.002, 0.05), 5),
                        "exit_below_pct": round(self.random.uniform(0.002, 0.05), 5),
                    },
                }
            )
        if selected == "relative_volume":
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "length": self.random.randint(8, 50),
                        "threshold": round(self.random.uniform(0.8, 2.5), 4),
                    },
                }
            )
        if selected == "roc_momentum":
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "length": self.random.randint(2, 30),
                        "buy_condition": self.random.choice(ROC_BUY_CONDITIONS),
                        "sell_condition": self.random.choice(ROC_SELL_CONDITIONS),
                        "buy_threshold_pct": round(self.random.uniform(-10.0, 15.0), 4),
                        "sell_threshold_pct": round(self.random.uniform(-15.0, 10.0), 4),
                    },
                }
            )
        if selected == "sar_trend":
            step = round(self.random.uniform(0.005, 0.06), 5)
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "step": step,
                        "max_step": round(self.random.uniform(max(step, 0.06), 0.4), 5),
                        "buy_condition": self.random.choice(SAR_BUY_CONDITIONS),
                        "sell_condition": self.random.choice(SAR_SELL_CONDITIONS),
                    },
                }
            )
        if selected == "ichimoku_trend":
            tenkan = self.random.randint(5, 15)
            kijun = self.random.randint(max(tenkan + 4, 18), 42)
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "tenkan": tenkan,
                        "kijun": kijun,
                        "senkou_b": self.random.randint(max(kijun + 8, 40), 90),
                        "buy_condition": self.random.choice(ICHI_BUY_CONDITIONS),
                        "sell_condition": self.random.choice(ICHI_SELL_CONDITIONS),
                    },
                }
            )
        if selected == "ttm_squeeze":
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "bb_length": self.random.randint(8, 36),
                        "bb_mult": round(self.random.uniform(1.0, 3.0), 4),
                        "kc_length": self.random.randint(8, 36),
                        "kc_mult": round(self.random.uniform(1.0, 2.5), 4),
                        "momentum_length": self.random.randint(8, 36),
                        "buy_condition": self.random.choice(TTM_BUY_CONDITIONS),
                        "sell_condition": self.random.choice(TTM_SELL_CONDITIONS),
                    },
                }
            )
        if selected == "heikin_ashi_trend":
            return normalize_rule(
                {
                    "kind": selected,
                    "timeframe": rule_timeframe,
                    "params": {
                        "mode": self.random.choice(("transition", "state")),
                        "doji_tolerance_pct": round(self.random.uniform(0.0, 0.25), 4),
                    },
                }
            )
        return normalize_rule({"kind": "ma_cross", "timeframe": rule_timeframe, "params": {}})

    def random_candidate(self, *, symbols: list[str], timeframe: str) -> StrategyCandidate:
        rule_count = self.random.randint(max(2, int(self.min_rules)), max(2, int(self.max_rules)))
        if rule_count <= len(RULE_KINDS):
            kinds = self.random.sample(list(RULE_KINDS), k=rule_count)
        else:
            kinds = [self.random.choice(RULE_KINDS) for _ in range(rule_count)]
        rules = [self.random_rule(kind, timeframe=self._random_timeframe(timeframe)) for kind in kinds]
        entry_threshold = self.random.randint(2, rule_count)
        exit_threshold = self.random.randint(1, min(3, rule_count))
        return build_combo_candidate(
            symbols=self._random_symbol_subset(symbols),
            timeframe=timeframe,
            rules=rules,
            entry_threshold=entry_threshold,
            exit_threshold=exit_threshold,
            atr_length=self.random.randint(5, 30),
            atr_stop_mult=round(self.random.uniform(1.0, 5.0), 4),
            atr_trailing=self.random.choice((True, True, False)),
            profit_target_atr_mult=self.random.choice((None, round(self.random.uniform(1.0, 5.0), 4))),
            max_position_pct=round(self.random.uniform(0.05, 0.20), 4),
            max_daily_loss_pct=round(self.random.uniform(0.015, 0.06), 4),
            max_trades_per_day=self.random.randint(3, 20),
        )

    def mutate_candidate(self, candidate: StrategyCandidate) -> StrategyCandidate:
        p = copy.deepcopy(candidate.parameters)
        rules = list(copy.deepcopy(p.get("rules") or []))
        if len(rules) < 2:
            return self.random_candidate(symbols=list(candidate.symbols), timeframe=candidate.timeframe)
        symbols = normalize_symbols(candidate.symbols)
        if self.random.random() < 0.30:
            symbols = self._mutate_symbols(symbols)

        if len(rules) < int(self.max_rules) and self.random.random() < 0.22:
            existing = {str(rule.get("kind") or "") for rule in rules}
            choices = [kind for kind in RULE_KINDS if kind not in existing] or list(RULE_KINDS)
            rules.append(self.random_rule(self.random.choice(choices)))
        if len(rules) > max(2, int(self.min_rules)) and self.random.random() < 0.18:
            del rules[self.random.randrange(len(rules))]
        if self.random.random() < 0.25:
            idx = self.random.randrange(len(rules))
            existing = {str(rule.get("kind") or "") for i, rule in enumerate(rules) if i != idx}
            choices = [kind for kind in RULE_KINDS if kind not in existing] or list(RULE_KINDS)
            rules[idx] = self.random_rule(self.random.choice(choices))
        if len(self.timeframes) > 1 and self.random.random() < 0.35:
            idx = self.random.randrange(len(rules))
            rules[idx] = self._maybe_mutate_rule_timeframe(rules[idx])

        tweak_count = self.random.randint(1, min(3, len(rules)))
        for idx in self.random.sample(range(len(rules)), k=tweak_count):
            rules[idx] = self._jitter_rule(rules[idx])
            if len(self.timeframes) > 1 and self.random.random() < 0.20:
                rules[idx] = self._maybe_mutate_rule_timeframe(rules[idx])

        rule_count = len(rules)
        entry_threshold = _clamp_int(
            int(p.get("entry_threshold") or rule_count) + self.random.choice((-1, 0, 1)),
            2,
            rule_count,
        )
        exit_threshold = _clamp_int(
            int(p.get("exit_threshold") or 1) + self.random.choice((-1, 0, 1)),
            1,
            rule_count,
        )
        risk = dict(candidate.risk or {})
        return build_combo_candidate(
            symbols=symbols,
            timeframe=candidate.timeframe,
            rules=rules,
            entry_threshold=entry_threshold,
            exit_threshold=exit_threshold,
            atr_length=_clamp_int(int(p.get("atr_length") or 14) + self.random.choice((-3, -1, 0, 1, 3)), 3, 60),
            atr_stop_mult=_clamp_float(float(p.get("atr_stop_mult") or 2.5) + self.random.uniform(-0.5, 0.5), 0.5, 8.0),
            atr_trailing=self.random.choice((bool(p.get("atr_trailing", True)), True, False)),
            profit_target_atr_mult=self._mutate_optional_target(risk.get("profit_target_atr_mult")),
            max_position_pct=_clamp_float(float(risk.get("max_position_pct") or 0.15) + self.random.uniform(-0.03, 0.03), 0.01, 1.0),
            max_daily_loss_pct=_clamp_float(float(risk.get("max_daily_loss_pct") or 0.03) + self.random.uniform(-0.01, 0.01), 0.001, 0.50),
            max_trades_per_day=_clamp_int(int(risk.get("max_trades_per_day") or 8) + self.random.choice((-2, -1, 0, 1, 2)), 1, 100),
        )

    def crossover_candidates(self, left: StrategyCandidate, right: StrategyCandidate) -> StrategyCandidate:
        left_rules = list(copy.deepcopy(left.parameters.get("rules") or []))
        right_rules = list(copy.deepcopy(right.parameters.get("rules") or []))
        pool = left_rules + right_rules
        self.random.shuffle(pool)
        rules: list[dict[str, Any]] = []
        seen_kinds: set[str] = set()
        for rule in pool:
            kind = str(rule.get("kind") or "")
            if kind in seen_kinds and self.random.random() < 0.75:
                continue
            rules.append(rule)
            seen_kinds.add(kind)
            if len(rules) >= int(self.max_rules):
                break
        while len(rules) < max(2, int(self.min_rules)):
            rules.append(self.random_rule(timeframe=self._random_timeframe(left.timeframe or right.timeframe)))

        rule_count = len(rules)
        entry_threshold = self.random.choice(
            (
                int(left.parameters.get("entry_threshold") or rule_count),
                int(right.parameters.get("entry_threshold") or rule_count),
                self.random.randint(2, rule_count),
            )
        )
        exit_threshold = self.random.choice(
            (
                int(left.parameters.get("exit_threshold") or 1),
                int(right.parameters.get("exit_threshold") or 1),
                self.random.randint(1, min(3, rule_count)),
            )
        )
        source = self.random.choice((left, right))
        p = source.parameters
        risk = source.risk
        child = build_combo_candidate(
            symbols=self._crossover_symbols(left, right),
            timeframe=left.timeframe or right.timeframe,
            rules=rules,
            entry_threshold=entry_threshold,
            exit_threshold=exit_threshold,
            atr_length=int(p.get("atr_length") or 14),
            atr_stop_mult=float(p.get("atr_stop_mult") or 2.5),
            atr_trailing=bool(p.get("atr_trailing", True)),
            profit_target_atr_mult=risk.get("profit_target_atr_mult"),
            max_position_pct=float(risk.get("max_position_pct") or 0.15),
            max_daily_loss_pct=float(risk.get("max_daily_loss_pct") or 0.03),
            max_trades_per_day=int(risk.get("max_trades_per_day") or 8),
        )
        return self.mutate_candidate(child) if self.random.random() < 0.65 else child

    def _jitter_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        out = copy.deepcopy(rule)
        params = dict(out.get("params") or {})
        for key, value in list(params.items()):
            if isinstance(value, bool):
                if self.random.random() < 0.20:
                    params[key] = not value
            elif isinstance(value, int):
                delta = self.random.choice((-5, -3, -2, -1, 1, 2, 3, 5))
                params[key] = value + delta
            elif isinstance(value, float):
                span = max(0.01, abs(value) * 0.25)
                params[key] = round(value + self.random.uniform(-span, span), 5)
            elif key == "ma_type":
                params[key] = self.random.choice(("ema", "sma"))
        out["params"] = params
        out["timeframe"] = _rule_timeframe(out)
        return normalize_rule(out, default_timeframe=out["timeframe"])

    def _mutate_optional_target(self, value: Any) -> Optional[float]:
        if value is None:
            return None if self.random.random() < 0.60 else round(self.random.uniform(1.0, 5.0), 4)
        if self.random.random() < 0.20:
            return None
        return _clamp_float(float(value or 2.0) + self.random.uniform(-0.75, 0.75), 0.5, 8.0)

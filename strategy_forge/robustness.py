from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .backtest_runner import BacktestConfig, BacktestResult, run_backtest
from .data_loader import OHLCVData
from .scoring import assign_grade
from .strategy_templates import StrategyCandidate, get_template


@dataclass
class RejectionConfig:
    min_trades: int = 100
    max_single_trade_profit_share: float = 0.20
    max_single_month_profit_share: float = 0.40
    max_drawdown: float = 0.30
    min_neighbor_return_ratio: float = 0.50
    min_symbol_pass_rate: float = 0.50


def evaluate_overfit_rejections(
    result: BacktestResult,
    *,
    config: Optional[RejectionConfig] = None,
    validation_metrics: Optional[dict[str, Any]] = None,
    out_of_sample_metrics: Optional[dict[str, Any]] = None,
    neighbor_results: Optional[list[BacktestResult]] = None,
    symbol_results: Optional[list[BacktestResult]] = None,
) -> dict[str, Any]:
    cfg = config or RejectionConfig()
    metrics = result.metrics
    reasons: list[str] = []
    if int(metrics.get("trade_count") or 0) < int(cfg.min_trades):
        reasons.append("trade_count_below_minimum")
    if float(metrics.get("largest_winning_trade_contribution") or 0.0) > float(cfg.max_single_trade_profit_share):
        reasons.append("single_trade_profit_concentration")
    if float(metrics.get("largest_month_profit_contribution") or 0.0) > float(cfg.max_single_month_profit_share):
        reasons.append("single_month_profit_concentration")
    if float(metrics.get("net_profit") or 0.0) <= 0:
        reasons.append("net_profit_disappears_after_fees_slippage")
    if float(metrics.get("max_drawdown") or 0.0) > float(cfg.max_drawdown):
        reasons.append("max_drawdown_exceeds_limit")
    if validation_metrics is not None and float(validation_metrics.get("total_return") or 0.0) < 0:
        reasons.append("validation_performance_negative")
    if out_of_sample_metrics is not None and float(out_of_sample_metrics.get("total_return") or 0.0) < 0:
        reasons.append("out_of_sample_performance_negative")

    parameter_stability_score = 1.0
    if neighbor_results:
        base_return = max(0.000001, abs(float(metrics.get("total_return") or 0.0)))
        passing = 0
        for item in neighbor_results:
            if float(item.metrics.get("total_return") or 0.0) >= base_return * float(cfg.min_neighbor_return_ratio):
                passing += 1
        parameter_stability_score = passing / float(len(neighbor_results))
        if parameter_stability_score < 0.50:
            reasons.append("nearby_parameter_changes_destroy_performance")

    symbol_stability_score = 1.0
    if symbol_results:
        pass_count = sum(1 for item in symbol_results if float(item.metrics.get("total_return") or 0.0) > 0)
        symbol_stability_score = pass_count / float(len(symbol_results))
        if symbol_stability_score < float(cfg.min_symbol_pass_rate):
            reasons.append("strategy_only_works_on_one_symbol")

    equity_stability_score = equity_curve_stability(result.equity_curve)
    if equity_stability_score < 0.25:
        reasons.append("equity_curve_too_unstable")

    robustness_score = max(0.0, min(1.0, (parameter_stability_score + symbol_stability_score + equity_stability_score) / 3.0))
    rejected = bool(reasons)
    grade = assign_grade(metrics, rejected=rejected, weakness_count=len(reasons), robustness_score=robustness_score)
    return {
        "rejected": rejected,
        "reasons": reasons,
        "parameter_stability_score": parameter_stability_score,
        "symbol_stability_score": symbol_stability_score,
        "time_window_stability_score": equity_stability_score,
        "regime_score": regime_score(result),
        "monte_carlo_score": 0.0,
        "robustness_score": robustness_score,
        "final_grade": grade,
        "instability_penalty": 1.0 - robustness_score,
    }


def equity_curve_stability(equity_curve: list[dict[str, Any]]) -> float:
    if len(equity_curve) < 3:
        return 0.0
    values = [float(row.get("equity") or 0.0) for row in equity_curve]
    gains = 0
    for prev, cur in zip(values, values[1:]):
        if cur >= prev:
            gains += 1
    return gains / float(len(values) - 1)


def regime_score(result: BacktestResult) -> float:
    if not result.regime_summary:
        return 0.0
    profitable = 0
    counted = 0
    for row in result.regime_summary.values():
        if int(row.get("trade_count") or 0) <= 0:
            continue
        counted += 1
        if float(row.get("net_pnl") or 0.0) > 0:
            profitable += 1
    return profitable / float(counted) if counted else 0.0


def neighbor_candidates(candidate: StrategyCandidate, *, max_neighbors: int = 24) -> list[StrategyCandidate]:
    template = get_template(candidate.strategy_name)
    out: list[StrategyCandidate] = []
    for key, value in candidate.parameters.items():
        spec = template.parameter_space.get(key)
        if spec is None or spec.kind == "choice":
            continue
        step = float(spec.step or (1 if spec.kind == "int" else 0.1))
        for delta in (-2, -1, 1, 2):
            params = copy.deepcopy(candidate.parameters)
            new_value = float(value) + (delta * step)
            new_value = max(float(spec.min_value), min(float(spec.max_value), new_value))
            params[key] = int(round(new_value)) if spec.kind == "int" else round(new_value, 6)
            if params == candidate.parameters or not template.validate(params):
                continue
            out.append(
                StrategyCandidate(
                    strategy_name=candidate.strategy_name,
                    timeframe=candidate.timeframe,
                    symbols=list(candidate.symbols),
                    parameters=params,
                    entry_rule=copy.deepcopy(candidate.entry_rule),
                    exit_rule=copy.deepcopy(candidate.exit_rule),
                    risk=copy.deepcopy(candidate.risk),
                )
            )
            if len(out) >= int(max_neighbors):
                return out
    return out


def run_parameter_stability(
    candidate: StrategyCandidate,
    data: OHLCVData,
    *,
    backtest_config: Optional[BacktestConfig] = None,
    max_neighbors: int = 24,
    runner: Callable[[OHLCVData, StrategyCandidate, Optional[BacktestConfig]], BacktestResult] = run_backtest,
) -> list[BacktestResult]:
    return [runner(data, item, backtest_config) for item in neighbor_candidates(candidate, max_neighbors=max_neighbors)]

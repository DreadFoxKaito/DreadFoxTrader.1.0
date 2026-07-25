from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Optional

from .backtest_runner import BacktestConfig, BacktestResult, run_backtest, sharpe_ratio, sortino_ratio
from .data_loader import OHLCVData
from .strategy_templates import StrategyCandidate


@dataclass(frozen=True)
class WalkForwardSplit:
    train_start: int
    train_end: int
    validation_start: int
    validation_end: int
    test_start: int
    test_end: int


def generate_walk_forward_splits(
    data: OHLCVData,
    *,
    train_days: int = 90,
    validation_days: int = 30,
    test_days: int = 30,
    step_days: int = 30,
) -> list[WalkForwardSplit]:
    times = data.parsed_times()
    valid = [(i, t) for i, t in enumerate(times) if t is not None]
    if len(valid) < 10:
        return _bar_based_splits(len(data))
    start = valid[0][1]
    last = valid[-1][1]
    assert start is not None and last is not None
    out: list[WalkForwardSplit] = []
    cursor = start
    while cursor + timedelta(days=train_days + validation_days + test_days) <= last + timedelta(seconds=1):
        train_end_time = cursor + timedelta(days=train_days)
        validation_end_time = train_end_time + timedelta(days=validation_days)
        test_end_time = validation_end_time + timedelta(days=test_days)
        split = WalkForwardSplit(
            train_start=_first_index_at_or_after(times, cursor),
            train_end=_first_index_at_or_after(times, train_end_time),
            validation_start=_first_index_at_or_after(times, train_end_time),
            validation_end=_first_index_at_or_after(times, validation_end_time),
            test_start=_first_index_at_or_after(times, validation_end_time),
            test_end=_first_index_at_or_after(times, test_end_time),
        )
        if split.train_end > split.train_start and split.validation_end > split.validation_start and split.test_end > split.test_start:
            out.append(split)
        cursor = cursor + timedelta(days=step_days)
    return out


def _first_index_at_or_after(times: list[Any], target: Any) -> int:
    for i, ts in enumerate(times):
        if ts is not None and ts >= target:
            return i
    return len(times)


def _bar_based_splits(n: int) -> list[WalkForwardSplit]:
    if n < 60:
        return []
    train = max(10, int(n * 0.45))
    val = max(5, int(n * 0.15))
    test = max(5, int(n * 0.15))
    step = max(5, test)
    out: list[WalkForwardSplit] = []
    start = 0
    while start + train + val + test <= n:
        out.append(
            WalkForwardSplit(
                train_start=start,
                train_end=start + train,
                validation_start=start + train,
                validation_end=start + train + val,
                test_start=start + train + val,
                test_end=start + train + val + test,
            )
        )
        start += step
    return out


def run_walk_forward(
    candidate: StrategyCandidate,
    data: OHLCVData,
    *,
    backtest_config: Optional[BacktestConfig] = None,
    train_days: int = 90,
    validation_days: int = 30,
    test_days: int = 30,
    step_days: int = 30,
    runner: Callable[[OHLCVData, StrategyCandidate, Optional[BacktestConfig]], BacktestResult] = run_backtest,
) -> dict[str, Any]:
    splits = generate_walk_forward_splits(
        data,
        train_days=train_days,
        validation_days=validation_days,
        test_days=test_days,
        step_days=step_days,
    )
    windows: list[dict[str, Any]] = []
    test_returns: list[float] = []
    validation_returns: list[float] = []
    drawdowns: list[float] = []
    equity_returns: list[float] = []
    for split in splits:
        train = runner(data.subset(split.train_start, split.train_end), candidate, backtest_config)
        validation = runner(data.subset(split.validation_start, split.validation_end), candidate, backtest_config)
        test = runner(data.subset(split.test_start, split.test_end), candidate, backtest_config)
        tr = float(test.metrics.get("total_return") or 0.0)
        vr = float(validation.metrics.get("total_return") or 0.0)
        test_returns.append(tr)
        validation_returns.append(vr)
        drawdowns.append(float(test.metrics.get("max_drawdown") or 0.0))
        equity_returns.extend(_curve_returns(test.equity_curve))
        windows.append(
            {
                "train_return": float(train.metrics.get("total_return") or 0.0),
                "validation_return": vr,
                "test_return": tr,
                "test_drawdown": float(test.metrics.get("max_drawdown") or 0.0),
                "test_trade_count": int(test.metrics.get("trade_count") or 0),
            }
        )
    profitable = sum(1 for value in test_returns if value > 0)
    avg_return = sum(test_returns) / float(len(test_returns)) if test_returns else 0.0
    worst_return = min(test_returns) if test_returns else 0.0
    profitable_pct = profitable / float(len(test_returns)) if test_returns else 0.0
    avg_validation = sum(validation_returns) / float(len(validation_returns)) if validation_returns else 0.0
    wf_sharpe = sharpe_ratio(equity_returns, data.timeframe)
    wf_sortino = sortino_ratio(equity_returns, data.timeframe)
    dd_consistency = 1.0 - (max(drawdowns) - min(drawdowns)) if drawdowns else 0.0
    oos_score = (avg_return * 0.45) + (profitable_pct * 0.35) + (max(0.0, worst_return) * 0.20)
    return {
        "windows": windows,
        "window_count": len(windows),
        "average_walk_forward_return": avg_return,
        "worst_walk_forward_return": worst_return,
        "profitable_window_pct": profitable_pct,
        "walk_forward_sharpe": wf_sharpe,
        "walk_forward_sortino": wf_sortino,
        "parameter_drift": 0.0,
        "drawdown_consistency": dd_consistency,
        "validation_score": avg_validation,
        "out_of_sample_score": oos_score,
        "walk_forward_score": oos_score,
    }


def _curve_returns(equity_curve: list[dict[str, Any]]) -> list[float]:
    out: list[float] = []
    prev = None
    for row in equity_curve:
        cur = float(row.get("equity") or 0.0)
        if prev and prev > 0:
            out.append((cur / prev) - 1.0)
        prev = cur
    return out

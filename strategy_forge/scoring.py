from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def normalize_positive(value: float, target: float) -> float:
    return clamp(float(value) / float(target)) if target else 0.0


def normalize_profit_factor_edge(value: float) -> float:
    return clamp((min(max(float(value), 0.0), 5.0) - 1.0) / 3.0)


def normalize_win_rate_edge(value: float) -> float:
    return clamp((float(value) - 0.50) / 0.35)


def normalize_return(value: float) -> float:
    return clamp((float(value) + 0.10) / 0.60)


def normalize_drawdown(value: float, max_reasonable: float = 0.35) -> float:
    return clamp(float(value) / float(max_reasonable))


@dataclass
class ScoreBreakdown:
    profile: str
    score: float
    components: dict[str, float]


def score_metrics(metrics: dict[str, Any], *, profile: str = "balanced", instability_penalty: float = 0.0) -> ScoreBreakdown:
    total_return = float(metrics.get("total_return") or 0.0)
    sortino = float(metrics.get("sortino") or 0.0)
    sharpe = float(metrics.get("sharpe") or 0.0)
    pf = float(metrics.get("profit_factor") or 0.0)
    win_rate = float(metrics.get("win_rate") or 0.0)
    max_dd = float(metrics.get("max_drawdown") or 0.0)
    expectancy = float(metrics.get("expectancy") or 0.0)
    turnover = float(metrics.get("turnover") or 0.0)
    oos = float(metrics.get("out_of_sample_score") or metrics.get("validation_score") or 0.0)

    profile_key = str(profile or "balanced").lower()
    profiles = {
        "highest_return": total_return,
        "lowest_drawdown": 1.0 - normalize_drawdown(max_dd),
        "best_sharpe": normalize_positive(sharpe, 3.0),
        "best_sortino": normalize_positive(sortino, 4.0),
        "best_profit_factor": normalize_positive(min(pf, 5.0), 3.0),
        "best_expectancy": normalize_positive(expectancy, max(1.0, abs(expectancy) if expectancy > 0 else 1.0)),
        "best_out_of_sample": oos,
        "best_low_turnover": 1.0 - clamp(turnover / 0.20),
    }
    if profile_key in profiles and profile_key != "balanced":
        raw = float(profiles[profile_key])
        return ScoreBreakdown(profile_key, raw, {profile_key: raw})

    components = {
        "normalized_net_return": normalize_return(total_return),
        "normalized_sortino": normalize_positive(sortino, 4.0),
        "normalized_profit_factor": normalize_profit_factor_edge(pf),
        "normalized_win_rate": normalize_win_rate_edge(win_rate),
        "normalized_max_drawdown": normalize_drawdown(max_dd),
        "turnover_penalty": clamp(turnover / 0.20),
        "instability_penalty": clamp(instability_penalty),
    }
    score = (
        0.24 * components["normalized_net_return"]
        + 0.18 * components["normalized_sortino"]
        + 0.28 * components["normalized_profit_factor"]
        + 0.22 * components["normalized_win_rate"]
        - 0.30 * components["normalized_max_drawdown"]
        - 0.15 * components["turnover_penalty"]
        - 0.25 * components["instability_penalty"]
    )
    return ScoreBreakdown("balanced", score, components)


def assign_grade(
    metrics: dict[str, Any],
    *,
    rejected: bool,
    weakness_count: int = 0,
    walk_forward_score: float = 0.0,
    robustness_score: float = 0.0,
) -> str:
    if rejected:
        return "Reject"
    profitable = float(metrics.get("total_return") or 0.0) > 0
    acceptable_dd = float(metrics.get("max_drawdown") or 0.0) <= 0.30
    enough_trades = int(metrics.get("trade_count") or 0) > 0
    if profitable and acceptable_dd and enough_trades and walk_forward_score >= 0 and robustness_score >= 0.60 and weakness_count == 0:
        return "A"
    if profitable and acceptable_dd and enough_trades and weakness_count <= 1:
        return "B"
    if profitable:
        return "C"
    return "Reject"

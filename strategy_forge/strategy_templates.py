from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class ParameterRange:
    name: str
    kind: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    step: Optional[float] = None
    choices: tuple[Any, ...] = ()

    def grid_values(self) -> list[Any]:
        if self.kind == "choice":
            return list(self.choices)
        if self.min_value is None or self.max_value is None:
            return []
        step = self.step or 1
        values: list[Any] = []
        current = float(self.min_value)
        guard = 0
        while current <= float(self.max_value) + 1e-12 and guard < 10000:
            if self.kind == "int":
                values.append(int(round(current)))
            else:
                values.append(round(float(current), 6))
            current += float(step)
            guard += 1
        return values


@dataclass(frozen=True)
class StrategyTemplate:
    name: str
    description: str
    required_indicators: tuple[str, ...]
    parameter_space: dict[str, ParameterRange]
    entry_rule: dict[str, list[str]]
    exit_rule: dict[str, list[str]]
    risk: dict[str, Any]
    constraint: Callable[[dict[str, Any]], bool]
    constraint_description: str = ""

    def validate(self, parameters: dict[str, Any]) -> bool:
        for key, param in self.parameter_space.items():
            if key not in parameters:
                return False
            value = parameters[key]
            if param.kind == "choice":
                if value not in param.choices:
                    return False
                continue
            try:
                numeric = float(value)
            except Exception:
                return False
            if param.min_value is not None and numeric < float(param.min_value):
                return False
            if param.max_value is not None and numeric > float(param.max_value):
                return False
        return bool(self.constraint(parameters))


@dataclass
class StrategyCandidate:
    strategy_name: str
    timeframe: str
    symbols: list[str]
    parameters: dict[str, Any]
    entry_rule: dict[str, list[str]]
    exit_rule: dict[str, list[str]]
    risk: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "timeframe": self.timeframe,
            "symbols": list(self.symbols),
            "parameters": dict(self.parameters),
            "entry_rule": self.entry_rule,
            "exit_rule": self.exit_rule,
            "risk": dict(self.risk),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "StrategyCandidate":
        return cls(
            strategy_name=str(payload.get("strategy_name") or ""),
            timeframe=str(payload.get("timeframe") or ""),
            symbols=[str(s).upper() for s in payload.get("symbols", [])],
            parameters=dict(payload.get("parameters") or {}),
            entry_rule=dict(payload.get("entry_rule") or {}),
            exit_rule=dict(payload.get("exit_rule") or {}),
            risk=dict(payload.get("risk") or {}),
        )


def _ma_constraint(params: dict[str, Any]) -> bool:
    return int(params.get("ma_fast", params.get("ema_fast", 0))) < int(params.get("ma_slow", params.get("ema_slow", 0)))


def _macd_constraint(params: dict[str, Any]) -> bool:
    return _ma_constraint(params) and int(params.get("macd_fast", 0)) < int(params.get("macd_slow", 0))


def _ichimoku_constraint(params: dict[str, Any]) -> bool:
    return int(params.get("tenkan_period", 0)) < int(params.get("kijun_period", 0)) < int(params.get("senkou_b_period", 0))


def _always_valid(_: dict[str, Any]) -> bool:
    return True


def _risk_defaults(**overrides: Any) -> dict[str, Any]:
    risk = {
        "max_position_pct": 0.15,
        "max_daily_loss_pct": 0.03,
        "max_trades_per_day": 8,
        "atr_trailing_stop": True,
        "profit_target_atr_mult": None,
    }
    risk.update(overrides)
    return risk


def _ma_ranges() -> dict[str, ParameterRange]:
    return {
        "ma_type": ParameterRange("ma_type", "choice", choices=("ema", "sma")),
        "ma_fast": ParameterRange("ma_fast", "int", 5, 50, 1),
        "ma_slow": ParameterRange("ma_slow", "int", 40, 250, 1),
    }


def _atr_ranges() -> dict[str, ParameterRange]:
    return {
        "atr_length": ParameterRange("atr_length", "int", 5, 30, 1),
        "atr_stop_mult": ParameterRange("atr_stop_mult", "float", 1.0, 5.0, 0.25),
        "atr_trailing": ParameterRange("atr_trailing", "choice", choices=(True, False)),
    }


def build_candidate(template: StrategyTemplate, parameters: dict[str, Any], symbols: list[str], timeframe: str) -> StrategyCandidate:
    if not template.validate(parameters):
        raise ValueError(f"invalid parameters for {template.name}: {parameters}")
    return StrategyCandidate(
        strategy_name=template.name,
        timeframe=str(timeframe).lower(),
        symbols=[str(s).upper() for s in symbols],
        parameters=dict(parameters),
        entry_rule={key: list(value) for key, value in template.entry_rule.items()},
        exit_rule={key: list(value) for key, value in template.exit_rule.items()},
        risk=dict(template.risk),
    )


TEMPLATES: dict[str, StrategyTemplate] = {
    "ema_rsi_atr_trend": StrategyTemplate(
        name="ema_rsi_atr_trend",
        description="EMA/SMA + RSI + ATR trend strategy.",
        required_indicators=("ma_fast", "ma_slow", "rsi", "atr"),
        parameter_space={
            **_ma_ranges(),
            "rsi_length": ParameterRange("rsi_length", "int", 5, 30, 1),
            "rsi_buy_min": ParameterRange("rsi_buy_min", "int", 30, 60, 1),
            "rsi_exit": ParameterRange("rsi_exit", "int", 30, 55, 1),
            **_atr_ranges(),
        },
        entry_rule={"all": ["close > ma_fast", "ma_fast > ma_slow", "rsi > rsi_buy_min", "atr_derivative >= 0"]},
        exit_rule={"any": ["close < ma_fast", "atr_trailing_stop_hit", "rsi < rsi_exit"]},
        risk=_risk_defaults(),
        constraint=_ma_constraint,
        constraint_description="fast moving average must be less than slow moving average",
    ),
    "ema_macd_atr_trend": StrategyTemplate(
        name="ema_macd_atr_trend",
        description="EMA/SMA + MACD + ATR trend strategy.",
        required_indicators=("ma_fast", "ma_slow", "macd", "atr"),
        parameter_space={
            **_ma_ranges(),
            "macd_fast": ParameterRange("macd_fast", "int", 6, 16, 1),
            "macd_slow": ParameterRange("macd_slow", "int", 18, 35, 1),
            "macd_signal": ParameterRange("macd_signal", "int", 5, 15, 1),
            **_atr_ranges(),
        },
        entry_rule={"all": ["close > ma_fast", "ma_fast > ma_slow", "macd > macd_signal", "macd_hist >= 0"]},
        exit_rule={"any": ["close < ma_fast", "macd < macd_signal", "atr_trailing_stop_hit"]},
        risk=_risk_defaults(),
        constraint=_macd_constraint,
        constraint_description="MA fast < MA slow and MACD fast < MACD slow",
    ),
    "bollinger_trend_mean_reversion": StrategyTemplate(
        name="bollinger_trend_mean_reversion",
        description="Bollinger Bands mean-reversion strategy with trend filter.",
        required_indicators=("bollinger", "trend_ma", "atr"),
        parameter_space={
            "bb_length": ParameterRange("bb_length", "int", 10, 60, 1),
            "bb_std": ParameterRange("bb_std", "float", 1.5, 3.5, 0.1),
            "trend_ma_length": ParameterRange("trend_ma_length", "int", 40, 250, 1),
            "percent_b_buy": ParameterRange("percent_b_buy", "float", 0.0, 0.35, 0.05),
            "percent_b_exit": ParameterRange("percent_b_exit", "float", 0.45, 0.95, 0.05),
            **_atr_ranges(),
        },
        entry_rule={"all": ["close > trend_ma", "percent_b <= percent_b_buy"]},
        exit_rule={"any": ["close >= bb_middle", "percent_b >= percent_b_exit", "atr_trailing_stop_hit"]},
        risk=_risk_defaults(profit_target_atr_mult=2.0),
        constraint=_always_valid,
    ),
    "vwap_pullback_volume": StrategyTemplate(
        name="vwap_pullback_volume",
        description="VWAP pullback strategy with relative-volume confirmation.",
        required_indicators=("vwap", "relative_volume", "trend_ma", "atr"),
        parameter_space={
            "vwap_distance_pct": ParameterRange("vwap_distance_pct", "float", 0.001, 0.03, 0.001),
            "relative_volume_threshold": ParameterRange("relative_volume_threshold", "float", 1.0, 3.0, 0.1),
            "trend_ma_length": ParameterRange("trend_ma_length", "int", 20, 200, 1),
            **_atr_ranges(),
        },
        entry_rule={"all": ["close >= trend_ma", "close near/below vwap", "relative_volume >= threshold"]},
        exit_rule={"any": ["close > vwap + distance", "close < trend_ma", "atr_trailing_stop_hit"]},
        risk=_risk_defaults(),
        constraint=_always_valid,
    ),
    "donchian_atr_breakout": StrategyTemplate(
        name="donchian_atr_breakout",
        description="Donchian breakout strategy with ATR trailing stop.",
        required_indicators=("donchian", "atr"),
        parameter_space={
            "donchian_lookback": ParameterRange("donchian_lookback", "int", 10, 252, 1),
            "breakout_confirmation": ParameterRange(
                "breakout_confirmation",
                "choice",
                choices=("close_above_high", "high_above_high", "close_above_channel"),
            ),
            **_atr_ranges(),
        },
        entry_rule={"all": ["breakout above prior donchian high"]},
        exit_rule={"any": ["close < donchian_low", "atr_trailing_stop_hit"]},
        risk=_risk_defaults(),
        constraint=_always_valid,
    ),
    "ichimoku_cloud_trend": StrategyTemplate(
        name="ichimoku_cloud_trend",
        description="Ichimoku cloud trend-continuation strategy.",
        required_indicators=("ichimoku", "atr"),
        parameter_space={
            "tenkan_period": ParameterRange("tenkan_period", "int", 5, 20, 1),
            "kijun_period": ParameterRange("kijun_period", "int", 15, 60, 1),
            "senkou_b_period": ParameterRange("senkou_b_period", "int", 40, 120, 1),
            "cloud_confirmation": ParameterRange("cloud_confirmation", "choice", choices=(True, False)),
            **_atr_ranges(),
        },
        entry_rule={"all": ["tenkan > kijun", "close above cloud if enabled"]},
        exit_rule={"any": ["close < kijun", "close below cloud", "atr_trailing_stop_hit"]},
        risk=_risk_defaults(),
        constraint=_ichimoku_constraint,
        constraint_description="Tenkan < Kijun < Senkou B",
    ),
    "supertrend_rsi_confirmation": StrategyTemplate(
        name="supertrend_rsi_confirmation",
        description="Supertrend + RSI confirmation strategy.",
        required_indicators=("supertrend", "rsi", "atr"),
        parameter_space={
            "supertrend_atr_length": ParameterRange("supertrend_atr_length", "int", 5, 30, 1),
            "supertrend_multiplier": ParameterRange("supertrend_multiplier", "float", 1.0, 5.0, 0.25),
            "rsi_length": ParameterRange("rsi_length", "int", 5, 30, 1),
            "rsi_buy_min": ParameterRange("rsi_buy_min", "int", 30, 60, 1),
            "rsi_exit": ParameterRange("rsi_exit", "int", 30, 55, 1),
            **_atr_ranges(),
        },
        entry_rule={"all": ["close > supertrend", "supertrend direction up", "rsi > rsi_buy_min"]},
        exit_rule={"any": ["close < supertrend", "supertrend direction down", "rsi < rsi_exit"]},
        risk=_risk_defaults(),
        constraint=_always_valid,
    ),
}


def get_template(name: str) -> StrategyTemplate:
    try:
        return TEMPLATES[str(name).strip().lower()]
    except KeyError as exc:
        raise KeyError(f"unknown strategy template: {name}") from exc


def list_templates() -> list[str]:
    return sorted(TEMPLATES.keys())

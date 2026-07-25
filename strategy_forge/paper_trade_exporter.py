from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from . import DEFAULT_DATA_DIR, DEFAULT_DB_PATH, __version__
from .result_store import get_run


def indicatorforge_rules(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    name = str(candidate.get("strategy_name") or "")
    params = candidate.get("parameters") if isinstance(candidate.get("parameters"), dict) else {}
    if name == "indicator_combo_evolved" or str(params.get("strategy_kind") or "") == "open_indicator_combo":
        out: list[dict[str, Any]] = []
        for rule in params.get("rules") or []:
            if not isinstance(rule, dict):
                continue
            kind = str(rule.get("kind") or "").strip().lower()
            timeframe = str(rule.get("timeframe") or candidate.get("timeframe") or "").strip().lower()
            p = rule.get("params") if isinstance(rule.get("params"), dict) else {}
            if kind == "ma_cross":
                ma_type = str(p.get("ma_type") or "ema")
                fast = int(p.get("fast") or 12)
                slow = int(p.get("slow") or 26)
                out.append(
                    {
                        "name": f"{ma_type.upper()}{fast} evolved trend filter",
                        "kind": "ma",
                        "timeframe": timeframe,
                        "params": {"length": fast, "ma_type": ma_type, "buy_relation": "above", "sell_relation": "below"},
                    }
                )
                out.append(
                    {
                        "name": f"{ma_type.upper()}{slow} evolved trend guard",
                        "kind": "ma",
                        "timeframe": timeframe,
                        "params": {"length": slow, "ma_type": ma_type, "buy_relation": "above", "sell_relation": "below"},
                    }
                )
            elif kind == "rsi_momentum":
                out.append(
                    {
                        "name": "Evolved RSI momentum",
                        "kind": "rsi",
                        "timeframe": timeframe,
                        "params": {
                            "length": int(p.get("length") or 14),
                            "oversold": float(p.get("entry_min") or 55),
                            "overbought": float(p.get("exit_below") or 45),
                            "oversold_relation": "above",
                            "oversold_action": "buy",
                            "overbought_relation": "below",
                            "overbought_action": "sell",
                        },
                    }
                )
            elif kind == "macd_momentum":
                out.append(
                    {
                        "name": "Evolved MACD momentum",
                        "kind": "macd",
                        "timeframe": timeframe,
                        "params": {
                            "fast_length": int(p.get("fast") or 12),
                            "slow_length": int(p.get("slow") or 26),
                            "signal_length": int(p.get("signal") or 9),
                            "mode": "signal_cross",
                        },
                    }
                )
            elif kind in {"bollinger_pullback", "bollinger_breakout"}:
                pullback = kind == "bollinger_pullback"
                out.append(
                    {
                        "name": "Evolved Bollinger rule",
                        "kind": "bb",
                        "timeframe": timeframe,
                        "params": {
                            "length": int(p.get("length") or 20),
                            "std_mult": float(p.get("std_mult") or 2.0),
                            "buy_condition": "percent_b_below" if pullback else "percent_b_above",
                            "sell_condition": "percent_b_above" if pullback else "percent_b_below",
                            "percent_b_buy_threshold": float(p.get("entry_b") or (0.25 if pullback else 0.85)),
                            "percent_b_sell_threshold": float(p.get("exit_b") or (0.65 if pullback else 0.55)),
                        },
                    }
                )
            elif kind == "donchian_breakout":
                out.append(
                    {
                        "name": "Evolved Donchian breakout",
                        "kind": "donchian",
                        "timeframe": timeframe,
                        "params": {
                            "lookback": int(p.get("lookback") or 20),
                            "buy_condition": "high_above_upper" if bool(p.get("use_high_break")) else "close_above_upper",
                            "sell_condition": "close_below_lower",
                        },
                    }
                )
            elif kind == "supertrend_trend":
                out.append(
                    {
                        "name": "Evolved Supertrend trend",
                        "kind": "supertrend",
                        "timeframe": timeframe,
                        "params": {
                            "atr_length": int(p.get("atr_length") or 10),
                            "multiplier": float(p.get("multiplier") or 3.0),
                            "buy_condition": "trend_up",
                            "sell_condition": "trend_down",
                        },
                    }
                )
            elif kind == "vwap_filter":
                out.append(
                    {
                        "name": "Evolved VWAP filter",
                        "kind": "vwap",
                        "timeframe": timeframe,
                        "params": {
                            "buy_condition": "within_band",
                            "sell_condition": "exit_below",
                            "max_extension_pct": float(p.get("max_extension_pct") or 0.015),
                            "max_pullback_pct": float(p.get("max_pullback_pct") or 0.01),
                            "exit_below_pct": float(p.get("exit_below_pct") or 0.012),
                        },
                    }
                )
            elif kind == "relative_volume":
                out.append(
                    {
                        "name": "Evolved Relative Volume",
                        "kind": "relative_volume",
                        "timeframe": timeframe,
                        "params": {
                            "length": int(p.get("length") or 20),
                            "threshold": float(p.get("threshold") or 1.2),
                            "buy_condition": "above_threshold",
                            "sell_condition": "below_threshold",
                        },
                    }
                )
            elif kind == "rsi_derivative":
                out.append(
                    {
                        "name": "Evolved RSI derivative",
                        "kind": "rsi_d",
                        "timeframe": timeframe,
                        "params": {
                            "buy_above": float(p.get("buy_above") or 0.0),
                            "sell_below": float(p.get("sell_below") or 0.0),
                        },
                    }
                )
            elif kind == "roc_momentum":
                out.append(
                    {
                        "name": "Evolved ROC momentum",
                        "kind": "roc",
                        "timeframe": timeframe,
                        "params": {
                            "length": int(p.get("length") or 12),
                            "buy_condition": str(p.get("buy_condition") or "momentum_long"),
                            "sell_condition": str(p.get("sell_condition") or "momentum_short"),
                            "buy_threshold_pct": float(p.get("buy_threshold_pct") or 0.0),
                            "sell_threshold_pct": float(p.get("sell_threshold_pct") or 0.0),
                        },
                    }
                )
            elif kind == "sar_trend":
                out.append(
                    {
                        "name": "Evolved Parabolic SAR",
                        "kind": "sar",
                        "timeframe": timeframe,
                        "params": {
                            "step": float(p.get("step") or 0.02),
                            "max_step": float(p.get("max_step") or 0.2),
                            "buy_condition": str(p.get("buy_condition") or "trend_long"),
                            "sell_condition": str(p.get("sell_condition") or "trend_short"),
                        },
                    }
                )
            elif kind == "ichimoku_trend":
                out.append(
                    {
                        "name": "Evolved Ichimoku trend",
                        "kind": "ichimoku",
                        "timeframe": timeframe,
                        "params": {
                            "conversion_line_length": int(p.get("tenkan") or 9),
                            "base_line_length": int(p.get("kijun") or 26),
                            "leading_span_b_length": int(p.get("senkou_b") or 52),
                            "lagging_line_displacement": 26,
                            "buy_condition": str(p.get("buy_condition") or "strong_long_confirm"),
                            "sell_condition": str(p.get("sell_condition") or "strong_short_confirm"),
                            "block_condition": "hold",
                            "buy_conditions": [str(p.get("buy_condition") or "strong_long_confirm")],
                            "sell_conditions": [str(p.get("sell_condition") or "strong_short_confirm")],
                            "block_conditions": ["hold"],
                            "buy_match_mode": "all",
                            "sell_match_mode": "all",
                            "block_match_mode": "all",
                            "cloud_thickness_threshold_pct": 1.0,
                            "base_line_bounce_tolerance_pct": 0.35,
                            "delayed_cross_lookback": 3,
                        },
                    }
                )
            elif kind == "ttm_squeeze":
                out.append(
                    {
                        "name": "Evolved TTM Squeeze",
                        "kind": "ttm",
                        "timeframe": timeframe,
                        "params": {
                            "bb_length": int(p.get("bb_length") or 20),
                            "bb_mult": float(p.get("bb_mult") or 2.0),
                            "kc_length": int(p.get("kc_length") or 20),
                            "kc_mult": float(p.get("kc_mult") or 1.5),
                            "momentum_length": int(p.get("momentum_length") or 20),
                            "buy_condition": str(p.get("buy_condition") or "long_release"),
                            "sell_condition": str(p.get("sell_condition") or "short_release"),
                        },
                    }
                )
            elif kind == "heikin_ashi_trend":
                out.append(
                    {
                        "name": "Evolved Heikin Ashi trend",
                        "kind": "heikin_ashi",
                        "timeframe": timeframe,
                        "params": {
                            "mode": str(p.get("mode") or "transition"),
                            "doji_tolerance_pct": float(p.get("doji_tolerance_pct") or 0.0),
                        },
                    }
                )
        return out
    if name == "ema_rsi_atr_trend":
        ma_type = str(params.get("ma_type") or "ema")
        fast = int(params.get("ma_fast") or 13)
        slow = int(params.get("ma_slow") or 78)
        rsi_buy = float(params.get("rsi_buy_min") or 45)
        rsi_exit = float(params.get("rsi_exit") or 40)
        return [
            {
                "name": f"{ma_type.upper()}{fast} price filter",
                "kind": "ma",
                "params": {"length": fast, "ma_type": ma_type, "buy_relation": "above", "sell_relation": "below"},
            },
            {
                "name": f"{ma_type.upper()}{slow} guard",
                "kind": "ma",
                "params": {"length": slow, "ma_type": ma_type, "buy_relation": "above", "sell_relation": "below"},
            },
            {
                "name": "RSI confirmation",
                "kind": "rsi",
                "params": {
                    "oversold": rsi_buy,
                    "overbought": rsi_exit,
                    "oversold_relation": "above",
                    "oversold_action": "buy",
                    "overbought_relation": "below",
                    "overbought_action": "sell",
                },
            },
        ]
    return []


def export_run(
    run_id: int,
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    target: str = "paper_trade",
    output_path: str | Path | None = None,
    include_b_grade: bool = False,
) -> dict[str, Any]:
    run = get_run(run_id, db_path=db_path)
    if run is None:
        raise ValueError(f"run {run_id} not found")
    grade = str(run.get("final_grade") or "")
    allowed = {"A"} | ({"B"} if include_b_grade else set())
    if grade not in allowed:
        raise ValueError(f"run {run_id} has grade {grade}; only {sorted(allowed)} can be exported")
    candidate = run.get("candidate") or {}
    config = {
        "export_type": target,
        "strategy_forge_version": __version__,
        "source_run_id": int(run_id),
        "safety": {
            "research_or_paper_trade_only": True,
            "live_trading_enabled": False,
            "requires_explicit_live_action": True,
        },
        "symbols": candidate.get("symbols") or [run.get("symbol")],
        "timeframe": candidate.get("timeframe") or run.get("timeframe"),
        "strategy_forge_candidate": candidate,
        "indicator_rules_json": indicatorforge_rules(candidate),
        "trading_enabled": False,
        "paper_trade": True,
        "risk": (candidate.get("risk") if isinstance(candidate.get("risk"), dict) else {}),
    }
    if output_path is None:
        out_dir = DEFAULT_DATA_DIR / "exports"
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / f"strategy_forge_run_{run_id}_{target}.json"
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, sort_keys=True), encoding="utf-8")
    return {"path": str(path), "config": config}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export validated Strategy Forge runs for paper-trading tests.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--target", default="paper_trade")
    parser.add_argument("--output")
    parser.add_argument("--include-b-grade", action="store_true")
    args = parser.parse_args(argv)
    result = export_run(
        args.run_id,
        db_path=args.db_path,
        target=args.target,
        output_path=args.output,
        include_b_grade=args.include_b_grade,
    )
    print(result["path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

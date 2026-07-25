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
            p = rule.get("params") if isinstance(rule.get("params"), dict) else {}
            if kind == "ma_cross":
                ma_type = str(p.get("ma_type") or "ema")
                fast = int(p.get("fast") or 12)
                slow = int(p.get("slow") or 26)
                out.append(
                    {
                        "name": f"{ma_type.upper()}{fast} evolved trend filter",
                        "kind": "ma",
                        "params": {"length": fast, "ma_type": ma_type, "buy_relation": "above", "sell_relation": "below"},
                    }
                )
                out.append(
                    {
                        "name": f"{ma_type.upper()}{slow} evolved trend guard",
                        "kind": "ma",
                        "params": {"length": slow, "ma_type": ma_type, "buy_relation": "above", "sell_relation": "below"},
                    }
                )
            elif kind == "rsi_momentum":
                out.append(
                    {
                        "name": "Evolved RSI momentum",
                        "kind": "rsi",
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

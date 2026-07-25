from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from . import DEFAULT_DB_PATH
from .result_store import connect, get_run, get_trades


def build_report(run_id: int, *, db_path: str | Path = DEFAULT_DB_PATH) -> str:
    run = get_run(run_id, db_path=db_path)
    if run is None:
        return f"Run {run_id} not found."
    trades = get_trades(run_id, db_path=db_path)
    robustness = _latest_robustness(run_id, db_path=db_path)
    metrics = run.get("metrics") or {}
    params = run.get("parameters") or {}
    lines = [
        f"DreadFox Strategy Forge Report: run {run_id}",
        "",
        f"Template: {run['strategy_template']}",
        f"Symbol/timeframe: {run['symbol']} {run['timeframe']}",
        f"Grade: {run['final_grade']}",
        f"Dates: {run.get('start_date') or '-'} to {run.get('end_date') or '-'}",
        "",
        "Core metrics:",
        f"- Total return: {float(run['total_return']) * 100:.2f}%",
        f"- CAGR: {float(run['cagr']) * 100:.2f}%",
        f"- Sharpe / Sortino: {float(run['sharpe']):.3f} / {float(run['sortino']):.3f}",
        f"- Max drawdown: {float(run['max_drawdown']) * 100:.2f}%",
        f"- Win rate: {float(run['win_rate']) * 100:.2f}%",
        f"- Profit factor: {float(run['profit_factor']):.3f}",
        f"- Expectancy: {float(run['expectancy']):.2f}",
        f"- Trades: {int(run['trade_count'])}",
        f"- Fees/slippage: ${float(run['fees_paid']):.2f} / ${float(run['slippage_estimate']):.2f}",
        "",
        "Validation:",
        f"- In-sample score: {float(run['in_sample_score']):.4f}",
        f"- Validation score: {float(run['validation_score']):.4f}",
        f"- Out-of-sample score: {float(run['out_of_sample_score']):.4f}",
        f"- Walk-forward score: {float(run['walk_forward_score']):.4f}",
        f"- Robustness score: {float(run['robustness_score']):.4f}",
        "",
        "Parameters:",
    ]
    for key, value in sorted(params.items()):
        lines.append(f"- {key}: {value}")
    if robustness:
        details = json.loads(robustness.get("details_json") or "{}")
        lines.extend(["", "Robustness:"])
        for key in (
            "parameter_stability_score",
            "symbol_stability_score",
            "time_window_stability_score",
            "regime_score",
            "monte_carlo_score",
        ):
            lines.append(f"- {key}: {float(robustness.get(key) or 0.0):.3f}")
        if details.get("reasons"):
            lines.append("- Rejection/weakness reasons: " + ", ".join(details["reasons"]))
    monthly = metrics.get("monthly_return_distribution") or {}
    if monthly:
        lines.extend(["", "Monthly returns:"])
        for month, value in sorted(monthly.items()):
            lines.append(f"- {month}: {float(value) * 100:.2f}%")
    if trades:
        lines.extend(["", f"Trades: {len(trades)} stored"])
        for trade in trades[:10]:
            lines.append(
                f"- {trade['entry_time']} -> {trade['exit_time']} "
                f"{float(trade['net_pnl']):.2f} ({trade['exit_reason']})"
            )
        if len(trades) > 10:
            lines.append(f"- ... {len(trades) - 10} more")
    return "\n".join(lines)


def _latest_robustness(run_id: int, *, db_path: str | Path) -> dict[str, Any] | None:
    conn = connect(db_path)
    row = conn.execute(
        "SELECT * FROM robustness_tests WHERE run_id=? ORDER BY id DESC LIMIT 1",
        (int(run_id),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a Strategy Forge run report.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--run-id", type=int, required=True)
    args = parser.parse_args(argv)
    print(build_report(args.run_id, db_path=args.db_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

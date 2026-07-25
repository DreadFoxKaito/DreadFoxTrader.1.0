from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from . import DEFAULT_DB_PATH
from .result_store import connect, list_runs


def _pct(value: Any) -> str:
    try:
        return f"{float(value) * 100.0:8.2f}%"
    except Exception:
        return "       -"


def _num(value: Any) -> str:
    try:
        return f"{float(value):8.3f}"
    except Exception:
        return "       -"


def format_leaderboard(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No Strategy Forge runs found."
    lines = [
        "run_id grade template                     symbol tf    return      sharpe  sortino max_dd  pf      trades wf_score",
        "------ ----- ---------------------------- ------ ----- ----------- ------- ------- ------- ------- ------ --------",
    ]
    for row in rows:
        lines.append(
            f"{int(row['run_id']):6d} "
            f"{str(row['final_grade'])[:5]:5s} "
            f"{str(row['strategy_template'])[:28]:28s} "
            f"{str(row['symbol'])[:6]:6s} "
            f"{str(row['timeframe'])[:5]:5s} "
            f"{_pct(row['total_return'])} "
            f"{_num(row['sharpe'])} "
            f"{_num(row['sortino'])} "
            f"{_pct(row['max_drawdown'])} "
            f"{_num(row['profit_factor'])} "
            f"{int(row['trade_count']):6d} "
            f"{_num(row['walk_forward_score'])}"
        )
    return "\n".join(lines)


def parameter_regions(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    grade: str = "A",
    top: int = 250,
) -> dict[str, Any]:
    conn = connect(db_path)
    rows = conn.execute(
        """
        SELECT strategy_template, parameters_json
        FROM strategy_runs
        WHERE final_grade=?
        ORDER BY walk_forward_score DESC, robustness_score DESC, total_return DESC
        LIMIT ?
        """,
        (grade, int(top)),
    ).fetchall()
    conn.close()
    numeric: dict[str, list[float]] = defaultdict(list)
    categorical: Counter[str] = Counter()
    indicators: Counter[str] = Counter()
    for row in rows:
        indicators.update(str(row["strategy_template"]).split("_"))
        try:
            params = json.loads(str(row["parameters_json"] or "{}"))
        except Exception:
            params = {}
        for key, value in params.items():
            if isinstance(value, bool):
                categorical[f"{key}={value}"] += 1
            elif isinstance(value, (int, float)):
                numeric[key].append(float(value))
            else:
                categorical[f"{key}={value}"] += 1
    ranges: dict[str, dict[str, float]] = {}
    for key, values in numeric.items():
        if not values:
            continue
        ranges[key] = {
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / float(len(values)),
            "count": len(values),
        }
    return {
        "grade": grade,
        "sample_size": len(rows),
        "numeric_ranges": ranges,
        "categorical_counts": dict(categorical.most_common(20)),
        "common_indicator_words": dict(indicators.most_common(20)),
    }


def format_parameter_regions(summary: dict[str, Any]) -> str:
    if int(summary.get("sample_size") or 0) <= 0:
        return f"No {summary.get('grade', 'A')}-grade strategy regions yet."
    lines = [f"Parameter regions among {summary['sample_size']} {summary['grade']}-grade strategies:"]
    for key, row in sorted((summary.get("numeric_ranges") or {}).items()):
        lines.append(f"- {key}: {row['min']:.4g}-{row['max']:.4g} (avg {row['avg']:.4g}, n={int(row['count'])})")
    if summary.get("categorical_counts"):
        lines.append("Common categorical settings:")
        for key, count in summary["categorical_counts"].items():
            lines.append(f"- {key}: {count}")
    if summary.get("common_indicator_words"):
        lines.append("Common indicator/template terms:")
        for key, count in summary["common_indicator_words"].items():
            lines.append(f"- {key}: {count}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show DreadFox Strategy Forge leaderboard.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--sort", default="walk_forward_score")
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--regions", action="store_true", help="Show parameter regions among A-grade runs.")
    parser.add_argument("--grade", default="A")
    args = parser.parse_args(argv)

    rows = list_runs(db_path=args.db_path, sort=args.sort, top=args.top)
    print(format_leaderboard(rows))
    if args.regions:
        print()
        print(format_parameter_regions(parameter_regions(db_path=args.db_path, grade=args.grade)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

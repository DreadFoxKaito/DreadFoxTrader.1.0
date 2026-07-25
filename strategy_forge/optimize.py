from __future__ import annotations

import argparse
import copy
from pathlib import Path

from . import DEFAULT_DB_PATH
from .backtest_runner import BacktestConfig, BacktestResult, run_backtest
from .candidate_generator import generate_candidates
from .combo_search import (
    OpenComboGenerator,
    aggregate_result_metrics,
    candidate_signature,
    combo_search_score,
    grade_combo_metrics,
)
from .data_loader import load_ohlcv
from .leaderboard import format_leaderboard
from .result_store import list_runs, store_backtest_result, store_robustness_test
from .robustness import RejectionConfig, evaluate_overfit_rejections
from .scoring import score_metrics
from .strategy_templates import list_templates
from .walk_forward import run_walk_forward


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run DreadFox Strategy Forge optimization.")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--timeframes", nargs="+", required=True)
    parser.add_argument("--template", action="append", choices=list_templates(), help="Legacy fixed-template modes only.")
    parser.add_argument("--mode", choices=("evolve", "grid", "random", "optuna"), default="evolve")
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--data-dir")
    parser.add_argument("--data-file", help="Single CSV/JSON file, only valid with one symbol and one timeframe.")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--initial-capital", type=float, default=100000.0)
    parser.add_argument("--commission-pct", type=float, default=0.0005)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--min-trades", type=int, default=100)
    parser.add_argument("--skip-walk-forward", action="store_true")
    parser.add_argument("--top", type=int, default=10)
    args = parser.parse_args(argv)

    bt_config = BacktestConfig(
        initial_capital=float(args.initial_capital),
        commission_pct=float(args.commission_pct),
        slippage_bps=float(args.slippage_bps),
    )
    rejection_config = RejectionConfig(min_trades=int(args.min_trades))
    data_cache = {}
    evaluated = 0
    stored = 0
    for symbol in args.symbols:
        for timeframe in args.timeframes:
            path = args.data_file if len(args.symbols) == 1 and len(args.timeframes) == 1 else None
            data_cache[(symbol.upper(), timeframe.lower())] = load_ohlcv(
                symbol,
                timeframe,
                data_dir=args.data_dir,
                path=path,
                min_candles=80,
            )

    if args.mode == "evolve":
        generator = OpenComboGenerator(seed=args.seed, min_rules=2, max_rules=5)
        total_evolved_evaluated = 0
        for timeframe in args.timeframes:
            tf = timeframe.lower()
            datasets = [data_cache[(symbol.upper(), tf)] for symbol in args.symbols]
            active_symbols = [data.symbol.upper() for data in datasets]
            population_size = max(2, min(60, max(2, int(args.trials) // 5)))
            elite_count = max(1, min(10, population_size // 4))
            patience = max(3, min(40, int(args.trials) // max(1, population_size)))
            population = [
                generator.random_candidate(symbols=active_symbols, timeframe=tf)
                for _ in range(min(population_size, int(args.trials)))
            ]
            seen: set[str] = set()
            evaluated_rows: list[dict[str, object]] = []
            evaluated = 0
            generation = 0
            best_score = -999999.0
            stale_generations = 0

            def evaluate(candidate: object) -> dict[str, object] | None:
                try:
                    symbol_results = [run_backtest(data, copy.deepcopy(candidate), bt_config) for data in datasets]
                    metrics = aggregate_result_metrics(symbol_results)
                    return {
                        "candidate": candidate,
                        "metrics": metrics,
                        "score": combo_search_score(metrics, min_trades=int(args.min_trades)),
                    }
                except Exception:
                    return None

            while population and evaluated < int(args.trials) and stale_generations < patience:
                generation_best = -999999.0
                for candidate in population:
                    signature = candidate_signature(candidate)
                    if signature in seen:
                        continue
                    seen.add(signature)
                    row = evaluate(candidate)
                    evaluated += 1
                    if row is not None:
                        evaluated_rows.append(row)
                        generation_best = max(generation_best, float(row["score"]))
                    if evaluated >= int(args.trials):
                        break

                evaluated_rows.sort(
                    key=lambda row: (
                        float(row["score"]),
                        float(dict(row["metrics"]).get("total_return") or 0.0),
                        float(dict(row["metrics"]).get("worst_symbol_return") or 0.0),
                    ),
                    reverse=True,
                )
                if generation_best > best_score + 0.000001:
                    best_score = generation_best
                    stale_generations = 0
                else:
                    stale_generations += 1
                generation += 1
                if evaluated >= int(args.trials) or stale_generations >= patience:
                    break

                elites = [row["candidate"] for row in evaluated_rows[:elite_count]]
                next_population = []
                next_seen: set[str] = set()
                while len(next_population) < population_size and evaluated + len(next_population) < int(args.trials):
                    if len(elites) >= 2 and generator.random.random() < 0.60:
                        parents = generator.random.sample(elites, 2)
                        child = generator.crossover_candidates(parents[0], parents[1])
                    elif elites and generator.random.random() < 0.85:
                        child = generator.mutate_candidate(generator.random.choice(elites))
                    else:
                        child = generator.random_candidate(symbols=active_symbols, timeframe=tf)
                    signature = candidate_signature(child)
                    if signature in seen or signature in next_seen:
                        continue
                    next_seen.add(signature)
                    next_population.append(child)
                population = next_population

            for row in evaluated_rows[: max(1, int(args.top))]:
                metrics = dict(row["metrics"])
                grade, reasons, robustness_score = grade_combo_metrics(metrics, min_trades=int(args.min_trades))
                result = BacktestResult(
                    candidate=row["candidate"],
                    symbol=",".join(active_symbols),
                    timeframe=tf,
                    metrics=metrics,
                    trades=[],
                    equity_curve=[],
                )
                run_id = store_backtest_result(
                    result,
                    db_path=args.db_path,
                    in_sample_score=float(row["score"]),
                    robustness_score=float(robustness_score),
                    final_grade=grade,
                )
                store_robustness_test(
                    run_id,
                    {
                        "rejected": grade == "Reject",
                        "reasons": reasons,
                        "parameter_stability_score": 0.0,
                        "symbol_stability_score": 1.0 if float(metrics.get("worst_symbol_return") or 0.0) > 0 else 0.5,
                        "time_window_stability_score": 0.0,
                        "regime_score": 0.0,
                        "monte_carlo_score": 0.0,
                        "robustness_score": robustness_score,
                        "final_grade": grade,
                    },
                    db_path=args.db_path,
                )
                stored += 1
            total_evolved_evaluated += evaluated
            print(
                f"Evolved {len(evaluated_rows)} open-combo candidates for {','.join(active_symbols)} {tf}; "
                f"stored top {min(len(evaluated_rows), max(1, int(args.top)))} runs.",
                flush=True,
            )

        print(f"Evaluated {total_evolved_evaluated} candidates; stored {stored} runs in {Path(args.db_path)}")
        print()
        print(format_leaderboard(list_runs(db_path=args.db_path, sort="total_return", top=args.top)))
        return 0

    templates = args.template or ["ema_rsi_atr_trend"]
    for timeframe in args.timeframes:
        for candidate in generate_candidates(
            mode=args.mode,
            template_names=templates,
            symbols=[s.upper() for s in args.symbols],
            timeframe=timeframe,
            trials=args.trials,
            seed=args.seed,
        ):
            for symbol in args.symbols:
                data = data_cache[(symbol.upper(), timeframe.lower())]
                one_symbol_candidate = candidate
                one_symbol_candidate.symbols = [symbol.upper()]
                result = run_backtest(data, one_symbol_candidate, bt_config)
                evaluated += 1
                wf = {}
                if not args.skip_walk_forward:
                    wf = run_walk_forward(one_symbol_candidate, data, backtest_config=bt_config)
                    result.metrics.update(
                        {
                            "validation_score": wf.get("validation_score", 0.0),
                            "out_of_sample_score": wf.get("out_of_sample_score", 0.0),
                        }
                    )
                preliminary = evaluate_overfit_rejections(result, config=rejection_config)
                score = score_metrics(result.metrics, instability_penalty=preliminary["instability_penalty"])
                final_grade = preliminary["final_grade"]
                run_id = store_backtest_result(
                    result,
                    db_path=args.db_path,
                    in_sample_score=score.score,
                    validation_score=float(wf.get("validation_score") or 0.0),
                    out_of_sample_score=float(wf.get("out_of_sample_score") or 0.0),
                    walk_forward_score=float(wf.get("walk_forward_score") or 0.0),
                    robustness_score=float(preliminary["robustness_score"]),
                    final_grade=final_grade,
                )
                store_robustness_test(run_id, preliminary, db_path=args.db_path)
                stored += 1
                if stored % 25 == 0:
                    print(f"Stored {stored} Strategy Forge runs...", flush=True)

    print(f"Evaluated {evaluated} candidates; stored {stored} runs in {Path(args.db_path)}")
    print()
    print(format_leaderboard(list_runs(db_path=args.db_path, sort="walk_forward_score", top=args.top)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

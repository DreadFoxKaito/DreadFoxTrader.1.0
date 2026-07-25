from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from strategy_forge.backtest_runner import BacktestConfig, BacktestResult, max_drawdown, profit_factor, run_backtest
from strategy_forge.candidate_generator import CandidateGenerator
from strategy_forge.combo_search import COMBO_STRATEGY_NAME, OpenComboGenerator, build_combo_candidate, candidate_signature
from strategy_forge.data_loader import OHLCVData
from strategy_forge.paper_trade_exporter import export_run
from strategy_forge.result_store import get_run, get_trades, store_backtest_result
from strategy_forge.robustness import RejectionConfig, evaluate_overfit_rejections
from strategy_forge.scoring import score_metrics
from strategy_forge.strategy_templates import StrategyCandidate, get_template
from strategy_forge.walk_forward import generate_walk_forward_splits


def synthetic_data(count: int = 160, *, drift: float = 0.5) -> OHLCVData:
    timestamps = [f"2026-01-{1 + (i // 24):02d}T{i % 24:02d}:00:00Z" for i in range(count)]
    closes = [100.0 + (i * drift) for i in range(count)]
    opens = [closes[i - 1] if i else closes[i] for i in range(count)]
    highs = [max(o, c) + 1.0 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 1.0 for o, c in zip(opens, closes)]
    volumes = [1000.0 + (i % 7) * 100.0 for i in range(count)]
    return OHLCVData(
        symbol="TQQQ",
        timeframe="1h",
        timestamps=timestamps,
        opens=opens,
        highs=highs,
        lows=lows,
        closes=closes,
        volumes=volumes,
        sessions=["regular"] * count,
        source="synthetic",
    )


def ema_rsi_candidate() -> StrategyCandidate:
    template = get_template("ema_rsi_atr_trend")
    params = {
        "ma_type": "ema",
        "ma_fast": 5,
        "ma_slow": 20,
        "rsi_length": 5,
        "rsi_buy_min": 50,
        "rsi_exit": 35,
        "atr_length": 5,
        "atr_stop_mult": 3.0,
        "atr_trailing": True,
    }
    return StrategyCandidate(
        strategy_name=template.name,
        timeframe="1h",
        symbols=["TQQQ"],
        parameters=params,
        entry_rule=template.entry_rule,
        exit_rule=template.exit_rule,
        risk=template.risk,
    )


def open_combo_candidate() -> StrategyCandidate:
    return build_combo_candidate(
        symbols=["TQQQ"],
        timeframe="1h",
        rules=[
            {"kind": "ma_cross", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
            {"kind": "rsi_momentum", "params": {"length": 5, "entry_min": 50, "exit_below": 30}},
        ],
        entry_threshold=2,
        exit_threshold=1,
        atr_length=5,
        atr_stop_mult=3.0,
        atr_trailing=False,
    )


class StrategyForgeTests(unittest.TestCase):
    def test_random_parameter_generation_respects_constraints(self):
        generator = CandidateGenerator(seed=7)
        template = get_template("ema_rsi_atr_trend")
        for _ in range(100):
            params = generator.random_parameters(template)
            self.assertTrue(template.validate(params))
            self.assertLess(params["ma_fast"], params["ma_slow"])

    def test_invalid_parameter_constraints_are_rejected(self):
        template = get_template("ema_rsi_atr_trend")
        params = {
            "ma_type": "ema",
            "ma_fast": 80,
            "ma_slow": 40,
            "rsi_length": 14,
            "rsi_buy_min": 45,
            "rsi_exit": 40,
            "atr_length": 14,
            "atr_stop_mult": 2.5,
            "atr_trailing": True,
        }
        self.assertFalse(template.validate(params))

        macd = get_template("ema_macd_atr_trend")
        macd_params = {
            "ma_type": "ema",
            "ma_fast": 10,
            "ma_slow": 50,
            "macd_fast": 30,
            "macd_slow": 20,
            "macd_signal": 9,
            "atr_length": 14,
            "atr_stop_mult": 2.5,
            "atr_trailing": True,
        }
        self.assertFalse(macd.validate(macd_params))

    def test_no_lookahead_executes_after_signal_bar(self):
        data = synthetic_data(12, drift=0.0)
        data.closes[8] = 200.0
        data.opens[9] = 200.0
        candidate = ema_rsi_candidate()

        def fake_entry(_candidate, _data, _indicators, index):
            return index == 8

        with mock.patch("strategy_forge.backtest_runner.entry_signal", side_effect=fake_entry):
            result = run_backtest(data, candidate, BacktestConfig(commission_pct=0.0, slippage_bps=0.0))

        self.assertEqual(result.trades[0]["entry_time"], data.timestamps[9])
        self.assertNotEqual(result.trades[0]["entry_time"], data.timestamps[8])

    def test_scoring_prefers_better_risk_adjusted_metrics(self):
        strong = {
            "total_return": 0.35,
            "sortino": 3.0,
            "profit_factor": 2.0,
            "win_rate": 0.58,
            "max_drawdown": 0.08,
            "turnover": 0.04,
        }
        weak = dict(strong, total_return=-0.05, sortino=-1.0, profit_factor=0.7, max_drawdown=0.35)
        self.assertGreater(score_metrics(strong).score, score_metrics(weak).score)

    def test_drawdown_and_profit_factor_calculations(self):
        self.assertAlmostEqual(max_drawdown([100.0, 120.0, 90.0, 110.0]), 0.25)
        self.assertAlmostEqual(profit_factor([10.0, -5.0, 20.0]), 6.0)

    def test_walk_forward_split_generation(self):
        data = synthetic_data(240, drift=0.1)
        splits = generate_walk_forward_splits(data, train_days=3, validation_days=2, test_days=2, step_days=2)
        self.assertTrue(splits)
        first = splits[0]
        self.assertLess(first.train_start, first.train_end)
        self.assertLess(first.validation_start, first.validation_end)
        self.assertLess(first.test_start, first.test_end)

    def test_overfit_rejection_rules(self):
        result = BacktestResult(
            candidate=ema_rsi_candidate(),
            symbol="TQQQ",
            timeframe="1h",
            metrics={
                "trade_count": 1,
                "largest_winning_trade_contribution": 0.9,
                "largest_month_profit_contribution": 0.9,
                "net_profit": 100.0,
                "max_drawdown": 0.1,
                "total_return": 0.1,
            },
            trades=[],
            equity_curve=[{"equity": 100.0}, {"equity": 101.0}],
        )
        details = evaluate_overfit_rejections(result, config=RejectionConfig(min_trades=10))
        self.assertTrue(details["rejected"])
        self.assertIn("trade_count_below_minimum", details["reasons"])
        self.assertIn("single_trade_profit_concentration", details["reasons"])

    def test_synthetic_ema_rsi_atr_strategy_trades_and_profits(self):
        result = run_backtest(
            synthetic_data(),
            ema_rsi_candidate(),
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
        )
        self.assertGreaterEqual(result.metrics["trade_count"], 1)
        self.assertGreater(result.metrics["total_return"], 0.0)
        self.assertEqual(result.trades[0]["side"], "long")

    def test_open_combo_generator_creates_multi_indicator_variations(self):
        generator = OpenComboGenerator(seed=11, min_rules=2, max_rules=5)
        candidates = [generator.random_candidate(symbols=["TQQQ"], timeframe="1h") for _ in range(20)]
        self.assertTrue(all(candidate.strategy_name == COMBO_STRATEGY_NAME for candidate in candidates))
        self.assertTrue(all(len(candidate.parameters["rules"]) >= 2 for candidate in candidates))
        self.assertGreater(len({candidate_signature(candidate) for candidate in candidates}), 10)

    def test_open_combo_candidate_backtests_without_template(self):
        result = run_backtest(
            synthetic_data(),
            open_combo_candidate(),
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
        )
        self.assertEqual(result.candidate.strategy_name, COMBO_STRATEGY_NAME)
        self.assertGreaterEqual(result.metrics["trade_count"], 1)
        self.assertGreater(result.metrics["total_return"], 0.0)

    def test_result_database_writes_runs_and_trades(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "forge.sqlite3"
            result = run_backtest(
                synthetic_data(),
                ema_rsi_candidate(),
                BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
            )
            run_id = store_backtest_result(result, db_path=db_path, final_grade="A")
            run = get_run(run_id, db_path=db_path)
            trades = get_trades(run_id, db_path=db_path)
            self.assertIsNotNone(run)
            self.assertEqual(run["strategy_template"], "ema_rsi_atr_trend")
            self.assertTrue(trades)

    def test_export_format_is_paper_only_and_requires_validated_grade(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "forge.sqlite3"
            output = Path(tmp) / "export.json"
            result = run_backtest(
                synthetic_data(),
                ema_rsi_candidate(),
                BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
            )
            run_id = store_backtest_result(result, db_path=db_path, final_grade="A")
            exported = export_run(run_id, db_path=db_path, output_path=output)
            self.assertTrue(output.exists())
            self.assertTrue(exported["config"]["safety"]["research_or_paper_trade_only"])
            self.assertFalse(exported["config"]["safety"]["live_trading_enabled"])
            self.assertIn("indicator_rules_json", exported["config"])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from strategy_forge.backtest_runner import BacktestConfig, BacktestResult, max_drawdown, profit_factor, run_backtest
from strategy_forge.candidate_generator import CandidateGenerator
from strategy_forge.combo_search import (
    COMBO_STRATEGY_NAME,
    OpenComboGenerator,
    RULE_KINDS,
    aggregate_result_metrics,
    build_combo_candidate,
    candidate_rule_timeframes,
    candidate_signature,
    combo_search_score,
)
from strategy_forge.data_loader import OHLCVData
from strategy_forge.paper_trade_exporter import export_run, indicatorforge_rules
from strategy_forge.result_store import get_run, get_trades, store_backtest_result
from strategy_forge.robustness import RejectionConfig, evaluate_overfit_rejections
from strategy_forge.scoring import score_metrics
from strategy_forge.strategy_templates import StrategyCandidate, get_template
from strategy_forge.walk_forward import generate_walk_forward_splits


def synthetic_data(count: int = 160, *, drift: float = 0.5, timeframe: str = "1h") -> OHLCVData:
    timestamps = [f"2026-01-{1 + (i // 24):02d}T{i % 24:02d}:00:00Z" for i in range(count)]
    closes = [100.0 + (i * drift) for i in range(count)]
    opens = [closes[i - 1] if i else closes[i] for i in range(count)]
    highs = [max(o, c) + 1.0 for o, c in zip(opens, closes)]
    lows = [min(o, c) - 1.0 for o, c in zip(opens, closes)]
    volumes = [1000.0 + (i % 7) * 100.0 for i in range(count)]
    return OHLCVData(
        symbol="TQQQ",
        timeframe=timeframe,
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

    def test_strategy_forge_combo_score_prioritizes_pf_and_win_rate(self):
        base = {
            "total_return": 0.10,
            "worst_symbol_return": 0.06,
            "sortino": 1.0,
            "max_drawdown": 0.08,
            "trade_count": 100,
        }
        high_quality = dict(base, profit_factor=3.5, win_rate=0.78)
        low_quality = dict(base, profit_factor=1.1, win_rate=0.51)

        self.assertGreater(
            combo_search_score(high_quality, min_trades=100),
            combo_search_score(low_quality, min_trades=100),
        )

    def test_balanced_scoring_gives_more_weight_to_pf_and_win_rate(self):
        base = {
            "total_return": 0.12,
            "sortino": 1.5,
            "max_drawdown": 0.10,
            "turnover": 0.04,
        }
        high_quality = score_metrics(dict(base, profit_factor=3.5, win_rate=0.78))
        low_quality = score_metrics(dict(base, profit_factor=1.1, win_rate=0.51))

        self.assertGreater(high_quality.score, low_quality.score)
        self.assertGreater(high_quality.components["normalized_profit_factor"], low_quality.components["normalized_profit_factor"])
        self.assertGreater(high_quality.components["normalized_win_rate"], low_quality.components["normalized_win_rate"])

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

    def test_open_combo_generator_respects_configured_indicator_count_range(self):
        generator = OpenComboGenerator(seed=23, min_rules=6, max_rules=9)
        candidates = [generator.random_candidate(symbols=["TQQQ"], timeframe="1h") for _ in range(30)]
        counts = [len(candidate.parameters["rules"]) for candidate in candidates]
        self.assertTrue(all(6 <= count <= 9 for count in counts))
        self.assertTrue(any(count > 5 for count in counts))

    def test_strategy_forge_catalog_covers_indicatorforge_families(self):
        self.assertTrue(
            {
                "ma_cross",
                "rsi_momentum",
                "rsi_derivative",
                "macd_momentum",
                "bollinger_pullback",
                "bollinger_breakout",
                "donchian_breakout",
                "supertrend_trend",
                "vwap_filter",
                "relative_volume",
                "roc_momentum",
                "sar_trend",
                "ichimoku_trend",
                "ttm_squeeze",
                "heikin_ashi_trend",
            }.issubset(set(RULE_KINDS))
        )

    def test_open_combo_generator_assigns_and_mutates_rule_timeframes(self):
        generator = OpenComboGenerator(seed=17, min_rules=2, max_rules=5, timeframes=("5m", "1h"))
        candidates = [generator.random_candidate(symbols=["TQQQ"], timeframe="5m") for _ in range(40)]

        self.assertTrue(
            all(
                set(candidate_rule_timeframes(candidate)).issubset({"5m", "1h"})
                for candidate in candidates
            )
        )
        self.assertTrue(any(len(candidate_rule_timeframes(candidate)) > 1 for candidate in candidates))

        seed = build_combo_candidate(
            symbols=["TQQQ"],
            timeframe="5m",
            rules=[
                {"kind": "ma_cross", "timeframe": "5m", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
                {"kind": "rsi_momentum", "timeframe": "5m", "params": {"length": 5, "entry_min": 50, "exit_below": 30}},
            ],
            entry_threshold=2,
            exit_threshold=1,
        )
        mutations = [generator.mutate_candidate(seed) for _ in range(40)]
        self.assertTrue(any("1h" in candidate_rule_timeframes(candidate) for candidate in mutations))

    def test_open_combo_candidate_backtests_without_template(self):
        result = run_backtest(
            synthetic_data(),
            open_combo_candidate(),
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
        )
        self.assertEqual(result.candidate.strategy_name, COMBO_STRATEGY_NAME)
        self.assertGreaterEqual(result.metrics["trade_count"], 1)
        self.assertGreater(result.metrics["total_return"], 0.0)

    def test_backtest_reports_one_share_dollar_return(self):
        result = run_backtest(
            synthetic_data(),
            open_combo_candidate(),
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
        )
        expected = sum(float(trade["exit_price"]) - float(trade["entry_price"]) for trade in result.trades)

        self.assertIn("one_share_net_profit", result.metrics)
        self.assertAlmostEqual(result.metrics["one_share_net_profit"], expected)
        self.assertAlmostEqual(
            result.metrics["one_share_buy_notional"],
            sum(float(trade["entry_price"]) for trade in result.trades),
        )

    def test_combo_aggregate_sums_one_share_dollar_return_across_symbols(self):
        result = run_backtest(
            synthetic_data(),
            open_combo_candidate(),
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
        )
        other = BacktestResult(
            candidate=result.candidate,
            symbol="SQQQ",
            timeframe=result.timeframe,
            metrics=dict(result.metrics),
            trades=list(result.trades),
            equity_curve=list(result.equity_curve),
        )

        aggregate = aggregate_result_metrics([result, other])

        self.assertAlmostEqual(aggregate["one_share_net_profit"], result.metrics["one_share_net_profit"] * 2.0)
        self.assertEqual(aggregate["symbol_one_share_net_profit"]["TQQQ"], result.metrics["one_share_net_profit"])
        self.assertEqual(aggregate["symbol_one_share_net_profit"]["SQQQ"], result.metrics["one_share_net_profit"])

    def test_open_combo_backtests_and_exports_all_indicatorforge_families(self):
        candidate = build_combo_candidate(
            symbols=["TQQQ"],
            timeframe="1h",
            rules=[
                {"kind": "ma_cross", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
                {"kind": "rsi_momentum", "params": {"length": 5, "entry_min": 50, "exit_below": 30}},
                {"kind": "rsi_derivative", "params": {"length": 5, "buy_above": -0.1, "sell_below": -0.1}},
                {"kind": "macd_momentum", "params": {"fast": 5, "slow": 18, "signal": 5, "hist_min": -1.0}},
                {"kind": "bollinger_breakout", "params": {"length": 20, "std_mult": 2.0, "entry_b": 0.55, "exit_b": 0.25}},
                {"kind": "donchian_breakout", "params": {"lookback": 10, "use_high_break": False}},
                {"kind": "supertrend_trend", "params": {"atr_length": 7, "multiplier": 2.0}},
                {"kind": "vwap_filter", "params": {"max_extension_pct": 0.50, "max_pullback_pct": 0.01, "exit_below_pct": 0.02}},
                {"kind": "relative_volume", "params": {"length": 10, "threshold": 0.7}},
                {
                    "kind": "roc_momentum",
                    "params": {
                        "length": 5,
                        "buy_condition": "roc_positive",
                        "sell_condition": "roc_negative",
                        "buy_threshold_pct": 0.0,
                        "sell_threshold_pct": 0.0,
                    },
                },
                {
                    "kind": "sar_trend",
                    "params": {
                        "step": 0.02,
                        "max_step": 0.2,
                        "buy_condition": "price_above_sar",
                        "sell_condition": "price_below_sar",
                    },
                },
                {
                    "kind": "ichimoku_trend",
                    "params": {
                        "tenkan": 9,
                        "kijun": 26,
                        "senkou_b": 52,
                        "buy_condition": "tenkan_above_kijun",
                        "sell_condition": "tenkan_below_kijun",
                    },
                },
                {
                    "kind": "ttm_squeeze",
                    "params": {
                        "bb_length": 20,
                        "bb_mult": 2.0,
                        "kc_length": 20,
                        "kc_mult": 1.5,
                        "momentum_length": 12,
                        "buy_condition": "momentum_above_zero",
                        "sell_condition": "momentum_below_zero",
                    },
                },
                {"kind": "heikin_ashi_trend", "params": {"mode": "state", "doji_tolerance_pct": 0.0}},
            ],
            entry_threshold=2,
            exit_threshold=1,
            atr_trailing=False,
        )

        result = run_backtest(
            synthetic_data(240),
            candidate,
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
        )
        self.assertIn("trade_count", result.metrics)

        exported_kinds = {rule.get("kind") for rule in indicatorforge_rules(candidate.to_dict())}
        self.assertTrue(
            {
                "ma",
                "rsi",
                "rsi_d",
                "macd",
                "bb",
                "donchian",
                "supertrend",
                "vwap",
                "relative_volume",
                "roc",
                "sar",
                "ichimoku",
                "ttm",
                "heikin_ashi",
            }.issubset(exported_kinds)
        )

    def test_open_combo_backtest_requires_consensus_across_rule_timeframes(self):
        execution = synthetic_data(180, drift=0.4, timeframe="5m")
        higher_confirming = synthetic_data(180, drift=0.4, timeframe="1h")
        higher_blocking = synthetic_data(180, drift=-0.25, timeframe="1h")
        candidate = build_combo_candidate(
            symbols=["TQQQ"],
            timeframe="5m",
            rules=[
                {"kind": "ma_cross", "timeframe": "5m", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
                {"kind": "ma_cross", "timeframe": "1h", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
            ],
            entry_threshold=2,
            exit_threshold=1,
            atr_trailing=False,
        )

        blocked = run_backtest(
            execution,
            candidate,
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
            data_by_timeframe={"5m": execution, "1h": higher_blocking},
        )
        confirmed = run_backtest(
            execution,
            candidate,
            BacktestConfig(commission_pct=0.0, slippage_bps=0.0),
            data_by_timeframe={"5m": execution, "1h": higher_confirming},
        )

        self.assertEqual(blocked.metrics["trade_count"], 0)
        self.assertGreaterEqual(confirmed.metrics["trade_count"], 1)
        self.assertEqual(confirmed.metrics["rule_timeframes"], ["5m", "1h"])

    def test_open_combo_export_preserves_rule_timeframes(self):
        candidate = build_combo_candidate(
            symbols=["TQQQ"],
            timeframe="5m",
            rules=[
                {"kind": "ma_cross", "timeframe": "5m", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
                {"kind": "rsi_momentum", "timeframe": "1h", "params": {"length": 5, "entry_min": 50, "exit_below": 30}},
            ],
            entry_threshold=2,
            exit_threshold=1,
        )

        rules = indicatorforge_rules(candidate.to_dict())

        self.assertIn("5m", {rule.get("timeframe") for rule in rules})
        self.assertIn("1h", {rule.get("timeframe") for rule in rules})

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

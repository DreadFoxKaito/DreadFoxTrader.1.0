from __future__ import annotations

import unittest

import app.main as main
from strategy_forge.data_loader import OHLCVData
from strategy_forge.indicator_factory import IndicatorCache
from strategy_forge.pivot_points import (
    calculate_pivot_points,
    pivot_level_sequence,
    pivot_target_above_price,
    pivot_target_below_price,
)


class PivotPointCalculationTests(unittest.TestCase):
    def test_classic_pivot_levels_use_source_candle(self):
        levels = calculate_pivot_points([100.0, 110.0], [90.0, 100.0], [95.0, 105.0], source_index=-1)

        self.assertIsNotNone(levels)
        assert levels is not None
        self.assertAlmostEqual(levels.p, 105.0)
        self.assertAlmostEqual(levels.r1, 110.0)
        self.assertAlmostEqual(levels.s1, 100.0)
        self.assertAlmostEqual(levels.r2, 115.0)
        self.assertAlmostEqual(levels.s2, 95.0)
        self.assertAlmostEqual(levels.r3, 120.0)
        self.assertAlmostEqual(levels.s3, 90.0)

    def test_target_above_price_without_half_levels(self):
        levels = calculate_pivot_points([110.0], [100.0], [105.0], source_index=-1)
        assert levels is not None

        self.assertEqual(pivot_target_above_price(levels, 106.0, offset=1), ("R1", 110.0))
        self.assertEqual(pivot_target_above_price(levels, 106.0, offset=2), ("R2", 115.0))

    def test_target_above_price_with_half_levels(self):
        levels = calculate_pivot_points([110.0], [100.0], [105.0], source_index=-1)
        assert levels is not None

        seq = pivot_level_sequence(levels, include_half_levels=True)
        self.assertIn(("P/R1", 107.5), seq)
        self.assertEqual(pivot_target_above_price(levels, 106.0, offset=0.5, include_half_levels=True), ("P/R1", 107.5))
        self.assertEqual(pivot_target_above_price(levels, 106.0, offset=1, include_half_levels=True), ("R1", 110.0))

    def test_target_below_price(self):
        levels = calculate_pivot_points([110.0], [100.0], [105.0], source_index=-1)
        assert levels is not None

        self.assertEqual(pivot_target_below_price(levels, 104.0, offset=1), ("S1", 100.0))


class PivotPointAppIntegrationTests(unittest.TestCase):
    def test_eval_indicator_rule_pivot_above_p(self):
        rule = {
            "name": "Pivots",
            "kind": "pivot",
            "params": {"buy_condition": "above_p", "sell_condition": "below_p", "tolerance_pct": 0.25},
        }
        closes = [95.0, 105.0, 106.0]
        highs = [100.0, 110.0, 107.0]
        lows = [90.0, 100.0, 105.0]

        out = main._eval_indicator_rule(rule, closes, 106.0, highs=highs, lows=lows)

        self.assertTrue(out["buy_ok"])
        self.assertFalse(out["sell_ok"])
        self.assertIn("PIV P=105", out["value"])

    def test_eval_indicator_rule_pivot_cross_below(self):
        rule = {
            "name": "Pivots",
            "kind": "pivot",
            "params": {"buy_condition": "hold", "sell_condition": "cross_below_p"},
        }
        closes = [95.0, 105.5, 104.0]
        highs = [100.0, 110.0, 106.0]
        lows = [90.0, 100.0, 103.0]

        out = main._eval_indicator_rule(rule, closes, 104.0, highs=highs, lows=lows)

        self.assertTrue(out["buy_ignored"])
        self.assertTrue(out["sell_ok"])

    def test_chart_config_and_svg_render_pivot_levels(self):
        cfg = main._indicator_rules_chart_config([{"kind": "pivot", "params": {"include_half_levels": 1}}])

        self.assertTrue(cfg["has_pivot"])
        self.assertTrue(cfg["pivot_include_half_levels"])

        closes = [100.0, 105.0, 106.0, 107.0]
        highs = [101.0, 110.0, 108.0, 109.0]
        lows = [99.0, 100.0, 104.0, 105.0]
        svg = main._market_chart_svg(
            closes=closes,
            opens=closes,
            highs=highs,
            lows=lows,
            ma_lengths=[],
            ema_lengths=[],
            macd_configs=[],
            bb_configs=[],
            ttm_configs=[],
            roc_lengths=[],
            sar_configs=[],
            heikin_ashi_mode=False,
            required_points=3,
            show_price=True,
            show_rsi=False,
            show_drsi=False,
            d_ma_lengths=[],
            d_ema_lengths=[],
            ichimoku_configs=[],
            pivot_enabled=True,
            pivot_include_half_levels=True,
        )

        self.assertIn(">R1</text>", svg)
        self.assertIn("stroke-dasharray='6 4'", svg)

    def test_indicator_factory_exposes_pivot_points(self):
        data = OHLCVData(
            symbol="AAPL",
            timeframe="1h",
            timestamps=["1", "2"],
            opens=[95.0, 105.0],
            highs=[100.0, 110.0],
            lows=[90.0, 100.0],
            closes=[95.0, 105.0],
            volumes=[1.0, 1.0],
            sessions=["regular", "regular"],
        )
        factory = IndicatorCache(data)

        levels = factory.get("pivot_points", source_index=-1)

        self.assertAlmostEqual(levels["P"], 105.0)
        self.assertAlmostEqual(levels["R1"], 110.0)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import re
import unittest

import app.main as main
from strategy_forge.indicator_factory import supertrend
from strategy_forge.supertrend import (
    calculate_supertrend,
    calculate_true_range,
    calculate_wilder_atr,
    segment_supertrend_runs,
)


class SupertrendCalculationTests(unittest.TestCase):
    def test_wilder_atr_seed_and_recursive_values(self):
        true_ranges = [2.0, 3.0, 4.0, 8.0, 10.0]

        atr = calculate_wilder_atr(true_ranges, 3)

        self.assertEqual(atr[:2], [None, None])
        self.assertAlmostEqual(atr[2], 3.0)
        self.assertAlmostEqual(atr[3], (3.0 * 2.0 + 8.0) / 3.0)
        self.assertAlmostEqual(atr[4], (((3.0 * 2.0 + 8.0) / 3.0) * 2.0 + 10.0) / 3.0)

    def test_true_range_first_candle_uses_high_minus_low(self):
        trs = calculate_true_range([11.0, 12.0], [9.0, 8.0], [10.0, 9.0])

        self.assertEqual(trs[0], 2.0)
        self.assertEqual(trs[1], 4.0)

    def test_stateful_bands_direction_persistence_and_flips(self):
        closes = [10, 9, 8, 7, 6, 5, 7, 9, 11, 13, 12, 10, 8, 6, 4, 6, 8, 10, 12]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]

        points = calculate_supertrend(highs, lows, closes, 3, 1.0)

        self.assertEqual([p.direction for p in points[2:7]], [1.0, 1.0, 1.0, -1.0, -1.0])
        self.assertTrue(points[5].flip_down)
        self.assertTrue(points[7].flip_up)
        self.assertFalse(points[6].flip_up)
        self.assertFalse(points[6].flip_down)

        bullish = points[8]
        bearish = points[12]
        self.assertEqual(bullish.trend, bullish.final_lower)
        self.assertEqual(bearish.trend, bearish.final_upper)
        self.assertLess(float(bullish.trend), closes[8])
        self.assertGreater(float(bearish.trend), closes[12])
        self.assertGreaterEqual(float(points[4].final_lower), float(points[3].final_lower))
        self.assertLessEqual(float(points[13].final_upper), float(points[12].final_upper))

    def test_segment_supertrend_runs_splits_repeated_regimes(self):
        closes = [10, 9, 8, 7, 6, 5, 7, 9, 11, 13, 12, 10, 8, 6, 4, 6, 8, 10, 12]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]

        segments = segment_supertrend_runs(calculate_supertrend(highs, lows, closes, 3, 1.0))

        self.assertEqual(
            segments,
            [(1.0, 2, 4), (-1.0, 5, 6), (1.0, 7, 10), (-1.0, 11, 15), (1.0, 16, 18)],
        )

    def test_strategy_forge_supertrend_keeps_existing_tuple_contract(self):
        closes = [10, 9, 8, 7, 6, 5, 7, 9]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]

        trend, direction = supertrend(highs, lows, closes, 3, 1.0)

        self.assertEqual(len(trend), len(closes))
        self.assertEqual(len(direction), len(closes))
        self.assertEqual(direction[2:8], [1.0, 1.0, 1.0, -1.0, -1.0, 1.0])

    def test_live_current_candle_recalculation_does_not_mutate_closed_points(self):
        closes = [10, 9, 8, 7, 6, 5, 7, 9, 11, 13, 12, 10]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]

        closed = calculate_supertrend(highs[:-1], lows[:-1], closes[:-1], 3, 1.0)
        live = calculate_supertrend(highs, lows, closes, 3, 1.0)

        self.assertEqual(closed, live[:-1])


class SupertrendIndicatorForgeTests(unittest.TestCase):
    def test_existing_saved_config_evaluates_without_migration(self):
        closes = [10, 9, 8, 7, 6, 5, 7, 9]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        rule = {
            "kind": "supertrend",
            "params": {
                "atr_length": 3,
                "multiplier": 1.0,
                "buy_condition": "flip_up",
                "sell_condition": "trend_down",
            },
        }

        check = main._eval_indicator_rule(rule, closes, closes[-1], highs=highs, lows=lows)

        self.assertTrue(check["buy_ok"])
        self.assertFalse(check["sell_ok"])
        self.assertIn("ST(3,1.000)", check["value"])
        self.assertIn("Period=3", check["detail"])

    def test_chart_renders_separate_paths_and_flip_markers(self):
        closes = [10, 9, 8, 7, 6, 5, 7, 9, 11, 13, 12, 10, 8, 6, 4, 6, 8, 10, 12]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        opens = list(closes)

        svg = main._market_chart_svg(
            closes=closes,
            opens=opens,
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
            required_points=5,
            show_price=True,
            show_rsi=False,
            show_drsi=False,
            d_ma_lengths=[],
            d_ema_lengths=[],
            ichimoku_configs=[],
            supertrend_configs=[(3, 1.0)],
        )

        green_paths = re.findall(r"<path d='([^']+)' stroke='#22c55e' stroke-width='1.35'", svg)
        red_paths = re.findall(r"<path d='([^']+)' stroke='#ef4444' stroke-width='1.35'", svg)
        self.assertEqual(len(green_paths), 3)
        self.assertEqual(len(red_paths), 2)
        self.assertTrue(all(path.count("M") == 1 for path in green_paths + red_paths))
        self.assertIn("fill='#22c55e'", svg)
        self.assertIn("fill='#ef4444'", svg)

    def test_chart_preserves_multiple_supertrend_configurations(self):
        closes = [10, 9, 8, 7, 6, 5, 7, 9, 11, 13, 12, 10, 8, 6, 4, 6, 8, 10, 12]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        opens = list(closes)

        svg = main._market_chart_svg(
            closes=closes,
            opens=opens,
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
            required_points=5,
            show_price=True,
            show_rsi=False,
            show_drsi=False,
            d_ma_lengths=[],
            d_ema_lengths=[],
            ichimoku_configs=[],
            supertrend_configs=[(3, 1.0), (4, 1.5)],
        )

        self.assertGreaterEqual(svg.count("stroke='#22c55e' stroke-width='1.35'"), 2)
        self.assertIn("stroke-dasharray='3 2'", svg)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

import app.main as main


class DonchianMidpointTests(unittest.TestCase):
    def test_donchian_channels_include_prior_bar_midpoint(self):
        highs = [10.0, 12.0, 11.0]
        lows = [6.0, 8.0, 7.0]

        upper, lower, middle = main._market_donchian_channels(highs, lows, 2)

        self.assertIsNone(upper[0])
        self.assertIsNone(lower[0])
        self.assertIsNone(middle[0])
        self.assertEqual(upper[2], 12.0)
        self.assertEqual(lower[2], 6.0)
        self.assertEqual(middle[2], 9.0)

    def test_eval_rule_buy_above_midpoint_inside_channel(self):
        rule = {
            "kind": "donchian",
            "params": {
                "lookback": 2,
                "buy_condition": "above_mid_inside",
                "sell_condition": "hold",
            },
        }
        closes = [8.0, 10.0, 10.0]
        highs = [10.0, 12.0, 11.0]
        lows = [6.0, 8.0, 9.0]

        out = main._eval_indicator_rule(rule, closes, closes[-1], highs=highs, lows=lows)

        self.assertTrue(out["buy_ok"])
        self.assertTrue(out["sell_ignored"])
        self.assertIn("M=9", out["value"])

    def test_eval_rule_sell_below_midpoint_inside_channel(self):
        rule = {
            "kind": "donchian",
            "params": {
                "lookback": 2,
                "buy_condition": "hold",
                "sell_condition": "below_mid_inside",
            },
        }
        closes = [8.0, 10.0, 8.0]
        highs = [10.0, 12.0, 9.0]
        lows = [6.0, 8.0, 7.0]

        out = main._eval_indicator_rule(rule, closes, closes[-1], highs=highs, lows=lows)

        self.assertTrue(out["buy_ignored"])
        self.assertTrue(out["sell_ok"])
        self.assertIn("sell=below_mid_inside", out["detail"])

    def test_midpoint_conditions_require_price_inside_channel(self):
        self.assertFalse(
            main._donchian_condition_hit(
                "above_mid_inside",
                close_now=13.0,
                high_now=13.0,
                low_now=12.0,
                upper=12.0,
                lower=6.0,
                middle=9.0,
            )
        )
        self.assertFalse(
            main._donchian_condition_hit(
                "below_mid_inside",
                close_now=5.0,
                high_now=6.0,
                low_now=5.0,
                upper=12.0,
                lower=6.0,
                middle=9.0,
            )
        )

    def test_eval_rule_buy_when_both_channel_bands_are_rising(self):
        rule = {
            "kind": "donchian",
            "params": {
                "lookback": 2,
                "buy_condition": "channel_slope_up",
                "sell_condition": "hold",
            },
        }
        closes = [8.0, 10.0, 12.0, 12.0]
        highs = [10.0, 12.0, 14.0, 13.0]
        lows = [6.0, 8.0, 10.0, 11.0]

        out = main._eval_indicator_rule(rule, closes, closes[-1], highs=highs, lows=lows)

        self.assertTrue(out["buy_ok"])
        self.assertIn("buy=channel_slope_up", out["detail"])

    def test_eval_rule_sell_when_both_channel_bands_are_falling(self):
        rule = {
            "kind": "donchian",
            "params": {
                "lookback": 2,
                "buy_condition": "hold",
                "sell_condition": "channel_slope_down",
            },
        }
        closes = [12.0, 10.0, 8.0, 8.0]
        highs = [14.0, 12.0, 10.0, 9.0]
        lows = [10.0, 8.0, 6.0, 7.0]

        out = main._eval_indicator_rule(rule, closes, closes[-1], highs=highs, lows=lows)

        self.assertTrue(out["sell_ok"])
        self.assertIn("sell=channel_slope_down", out["detail"])

    def test_slope_midpoint_combinations_require_matching_slope_and_position(self):
        self.assertTrue(
            main._donchian_condition_hit(
                "slope_up_above_mid_inside",
                close_now=12.0,
                high_now=13.0,
                low_now=11.0,
                upper=14.0,
                lower=8.0,
                middle=11.0,
                prev_upper=12.0,
                prev_lower=6.0,
            )
        )
        self.assertFalse(
            main._donchian_condition_hit(
                "slope_up_above_mid_inside",
                close_now=10.0,
                high_now=13.0,
                low_now=9.0,
                upper=14.0,
                lower=8.0,
                middle=11.0,
                prev_upper=12.0,
                prev_lower=6.0,
            )
        )
        self.assertTrue(
            main._donchian_condition_hit(
                "slope_down_below_mid_inside",
                close_now=8.0,
                high_now=9.0,
                low_now=7.0,
                upper=12.0,
                lower=6.0,
                middle=9.0,
                prev_upper=14.0,
                prev_lower=8.0,
            )
        )

    def test_chart_renders_donchian_midpoint_line(self):
        closes = [8.0, 10.0, 10.0, 9.0]
        highs = [10.0, 12.0, 11.0, 10.0]
        lows = [6.0, 8.0, 9.0, 8.0]

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
            donchian_lookbacks=[2],
        )

        self.assertIn("stroke='#facc15'", svg)
        self.assertIn("stroke-dasharray='5 3'", svg)


if __name__ == "__main__":
    unittest.main()

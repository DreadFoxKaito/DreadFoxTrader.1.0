import re
import unittest
from unittest import mock

import app.main as main


def _rows(count):
    return [
        {
            "begins_at": f"2026-01-01T{i % 24:02d}:00:00Z",
            "close_price": str(float(i + 1)),
        }
        for i in range(count)
    ]


def _ohlc_rows(count, *, session="reg", start=0):
    return [
        {
            "begins_at": f"2026-01-01T{(start + i) % 24:02d}:00:00Z",
            "open_price": str(float(i + 1)),
            "high_price": str(float(i + 3)),
            "low_price": str(float(i)),
            "close_price": str(float(i + 2)),
            "session": session,
        }
        for i in range(count)
    ]


class BacktestHistoryTests(unittest.TestCase):
    def test_robinhood_1h_keeps_best_span_when_requested_count_is_too_high(self):
        counts = {
            ("hour", "3month", "regular"): 384,
            ("hour", "month", "regular"): 132,
            ("hour", "week", "regular"): 30,
            ("hour", "day", "regular"): 6,
        }

        def fake_history(symbol, *, interval, span, bounds):
            return _rows(counts.get((interval, span, bounds), 0))

        with (
            mock.patch.object(main, "rh", object()),
            mock.patch.object(main, "_rh_adapter_get_stock_historicals", fake_history),
            mock.patch.object(main, "_rh_adapter_get_10m_stock_historicals", None),
            mock.patch.object(main, "_market_fetch_quote", return_value=None),
        ):
            closes = main._market_fetch_closes("TQQQ", "1h", "robinhood", min_candles=450)

        self.assertEqual(len(closes), 384)
        self.assertEqual(closes[0], 1.0)
        self.assertEqual(closes[-1], 384.0)

    def test_robinhood_1h_effective_lookback_uses_verified_capacity(self):
        lookback, note = main._robinhood_effective_backtest_lookback("1h", 410, 37)
        self.assertEqual(lookback, 344)
        self.assertIn("requested 410", note or "")

    def test_robinhood_10m_backtest_fetch_does_not_count_live_quote_as_candle(self):
        def fake_10m(symbol, *, span, bounds, min_candles, allow_partial=False):
            self.assertTrue(allow_partial)
            return _rows(194)

        with (
            mock.patch.object(main, "rh", object()),
            mock.patch.object(main, "_rh_adapter_get_stock_historicals", mock.Mock()),
            mock.patch.object(main, "_rh_adapter_get_10m_stock_historicals", fake_10m),
            mock.patch.object(main, "_market_fetch_quote", return_value=999.0),
        ):
            closes = main._market_fetch_closes(
                "TQQQ",
                "10m",
                "robinhood",
                min_candles=195,
                append_live_quote=False,
            )

        self.assertEqual(len(closes), 194)
        self.assertNotEqual(closes[-1], 999.0)

    def test_robinhood_10m_fetch_can_resample_to_required_capacity(self):
        def fake_10m(symbol, *, span, bounds, min_candles, allow_partial=False):
            self.assertEqual(min_candles, 195)
            self.assertTrue(allow_partial)
            return _rows(195)

        with (
            mock.patch.object(main, "rh", object()),
            mock.patch.object(main, "_rh_adapter_get_stock_historicals", mock.Mock()),
            mock.patch.object(main, "_rh_adapter_get_10m_stock_historicals", fake_10m),
            mock.patch.object(main, "_market_fetch_quote", return_value=None),
        ):
            closes = main._market_fetch_closes(
                "TQQQ",
                "10m",
                "robinhood",
                min_candles=195,
                append_live_quote=False,
            )

        self.assertEqual(len(closes), 195)

    def test_robinhood_extended_ohlc_fetch_uses_real_extended_bounds(self):
        calls = []

        def fake_history(symbol, *, interval, span, bounds):
            calls.append((interval, span, bounds))
            if bounds == "extended":
                return _ohlc_rows(2, session="pre", start=4)
            return _ohlc_rows(3, session="reg")

        with (
            mock.patch.object(main, "rh", object()),
            mock.patch.object(main, "_rh_adapter_get_stock_historicals", fake_history),
            mock.patch.object(main, "_rh_adapter_get_10m_stock_historicals", None),
        ):
            opens, highs, lows, closes, rows, requested_bounds = main._market_fetch_ohlc(
                "TQQQ",
                "1h",
                "robinhood",
                min_candles=5,
                include_extended=True,
            )

        self.assertEqual(requested_bounds, "extended")
        self.assertIn(("hour", "day", "extended"), calls)
        self.assertEqual(len(rows), 5)
        self.assertEqual((opens[-1], highs[-1], lows[-1], closes[-1]), (2.0, 4.0, 1.0, 3.0))
        pre_count, post_count = main._market_extended_session_counts(rows)
        self.assertEqual(pre_count, 2)
        self.assertEqual(post_count, 0)

    def test_crypto_ohlc_fetch_uses_24_7_history_without_synthetic_live_candle(self):
        seen = []

        def fake_crypto_history(symbol, *, interval, span, bounds, info=None):
            seen.append((symbol, interval, span, bounds))
            return [
                {"open_price": "100", "high_price": "102", "low_price": "99", "close_price": "101"},
                {"open_price": "101", "high_price": "103", "low_price": "100", "close_price": "102"},
            ]

        with (
            mock.patch.object(main, "rh", object()),
            mock.patch.object(main, "_rh_adapter_get_crypto_historicals", fake_crypto_history),
        ):
            opens, highs, lows, closes, rows, requested_bounds = main._market_fetch_ohlc(
                "BTC",
                "10m",
                broker_hint="robinhood_crypto",
                min_candles=2,
            )

        self.assertEqual(requested_bounds, "24_7")
        self.assertEqual(seen[0][0], "BTC")
        self.assertEqual(seen[0][1], "10minute")
        self.assertEqual(seen[0][3], "24_7")
        self.assertEqual(closes, [101.0, 102.0])
        self.assertEqual(opens, [100.0, 101.0])
        self.assertEqual(highs, [102.0, 103.0])
        self.assertEqual(lows, [99.0, 100.0])
        self.assertEqual(len(rows), 2)

    def test_crypto_close_fetch_uses_24_7_history_and_live_quote(self):
        seen = []

        def fake_crypto_history(symbol, *, interval, span, bounds, info=None):
            seen.append((symbol, interval, span, bounds))
            return [
                {"close_price": "101"},
                {"close_price": "102"},
            ]

        with (
            mock.patch.object(main, "rh", object()),
            mock.patch.object(main, "_rh_adapter_get_crypto_historicals", fake_crypto_history),
            mock.patch.object(main, "_market_fetch_crypto_quote", return_value=105.0),
        ):
            closes = main._market_fetch_closes(
                "BTC",
                "10m",
                broker_hint="robinhood_crypto",
                min_candles=2,
            )

        self.assertEqual(seen[0], ("BTC", "10minute", "week", "24_7"))
        self.assertEqual(closes, [101.0, 102.0, 105.0])

    def test_indicatorforge_preview_renders_unsaved_rules_without_500(self):
        def fake_ohlc(symbol, timeframe, broker_hint="robinhood", *, min_candles=0, include_extended=False):
            rows = _ohlc_rows(max(40, int(min_candles or 0)))
            opens, highs, lows, closes = main._market_extract_ohlc(rows)
            return opens, highs, lows, closes, rows, "regular"

        with (
            mock.patch.object(main, "_ensure_robinhood_markets_session", return_value=(True, "")),
            mock.patch.object(main, "_market_fetch_ohlc", fake_ohlc),
        ):
            html = main._render_indicatorforge_preview_html(
                timeframe="1h",
                symbols="TQQQ",
                rules_json='[{"kind":"ma","params":{"length":20}}]',
                broker_hint="robinhood",
                include_extended_hours_data=False,
            )

        self.assertIn("TQQQ", html)
        self.assertIn("Configured Indicator Rules", html)

    def test_indicatorforge_crypto_preview_uses_24_7_crypto_source(self):
        calls = []

        def fake_ohlc(symbol, timeframe, broker_hint="robinhood", *, min_candles=0, include_extended=False):
            calls.append(
                {
                    "symbol": symbol,
                    "broker_hint": broker_hint,
                    "include_extended": include_extended,
                }
            )
            rows = _ohlc_rows(max(40, int(min_candles or 0)))
            opens, highs, lows, closes = main._market_extract_ohlc(rows)
            return opens, highs, lows, closes, rows, "24_7"

        with (
            mock.patch.object(main, "_ensure_robinhood_markets_session", return_value=(True, "")),
            mock.patch.object(main, "_market_fetch_ohlc", fake_ohlc),
            mock.patch.object(main, "_market_fetch_crypto_quote", return_value=None),
        ):
            html = main._render_indicatorforge_preview_html(
                timeframe="10m",
                symbols="BTC",
                rules_json='[{"kind":"ma","params":{"length":2,"buy_relation":"above","sell_relation":"below"}}]',
                broker_hint="robinhood_crypto",
                include_extended_hours_data=False,
            )

        self.assertEqual(calls[0]["broker_hint"], "robinhood_crypto")
        self.assertTrue(calls[0]["include_extended"])
        self.assertIn("Robinhood crypto market data (24/7 candles)", html)
        self.assertNotIn("extended candles off", html)

    def test_indicatorforge_crypto_preview_matches_live_close_series_rule_input(self):
        check_calls = []
        chart_calls = []

        def fake_ohlc(symbol, timeframe, broker_hint="robinhood", *, min_candles=0, include_extended=False):
            rows = [
                {
                    "open_price": str(100 + i),
                    "high_price": str(150 + i),
                    "low_price": str(50 + i),
                    "close_price": str(101 + i),
                }
                for i in range(max(40, int(min_candles or 0)))
            ]
            opens, highs, lows, closes = main._market_extract_ohlc(rows)
            return opens, highs, lows, closes, rows, "24_7"

        def fake_checks(rules, closes, price, **kwargs):
            check_calls.append({"closes": list(closes), "price": price, "kwargs": dict(kwargs)})
            return [
                {
                    "name": "MA",
                    "_rule_kind": "ma",
                    "buy_ok": True,
                    "sell_ok": False,
                    "buy_ignored": False,
                    "sell_ignored": False,
                    "value": "ok",
                    "detail": "",
                }
            ]

        def fake_chart(**kwargs):
            chart_calls.append(dict(kwargs))
            return "<svg></svg>"

        with (
            mock.patch.object(main, "_ensure_robinhood_markets_session", return_value=(True, "")),
            mock.patch.object(main, "_market_fetch_ohlc", fake_ohlc),
            mock.patch.object(main, "_market_fetch_crypto_quote", return_value=105.0),
            mock.patch.object(main, "_build_indicator_rule_checks", fake_checks),
            mock.patch.object(main, "_market_chart_svg", fake_chart),
        ):
            html = main._render_indicatorforge_preview_html(
                timeframe="10m",
                symbols="BTC",
                rules_json='[{"name":"MA","kind":"ma","params":{"length":2,"buy_relation":"above","sell_relation":"below"}}]',
                broker_hint="robinhood_crypto",
                include_extended_hours_data=False,
            )

        self.assertIn("BTC", html)
        self.assertEqual(check_calls[0]["closes"][-1], 105.0)
        self.assertGreaterEqual(len(check_calls[0]["closes"]), 41)
        self.assertEqual(check_calls[0]["price"], 105.0)
        self.assertIn("opens", check_calls[0]["kwargs"])
        self.assertIn("highs", check_calls[0]["kwargs"])
        self.assertIn("lows", check_calls[0]["kwargs"])
        self.assertIs(check_calls[0]["kwargs"]["apply_overrides"], False)
        self.assertEqual(check_calls[0]["kwargs"]["opens"][-1], check_calls[0]["closes"][-2])
        self.assertEqual(
            check_calls[0]["kwargs"]["highs"][-1],
            max(check_calls[0]["closes"][-2], check_calls[0]["closes"][-1]),
        )
        self.assertEqual(
            check_calls[0]["kwargs"]["lows"][-1],
            min(check_calls[0]["closes"][-2], check_calls[0]["closes"][-1]),
        )
        self.assertIn("opens", chart_calls[0])
        self.assertIn("highs", chart_calls[0])
        self.assertIn("lows", chart_calls[0])

    def test_indicatorforge_preview_ignores_false_current_candle_param(self):
        def fake_ohlc(symbol, timeframe, broker_hint="robinhood", *, min_candles=0, include_extended=False):
            rows = _ohlc_rows(40)
            opens, highs, lows, closes = main._market_extract_ohlc(rows)
            return opens, highs, lows, closes, rows, "regular"

        with (
            mock.patch.object(main, "_ensure_robinhood_markets_session", return_value=(True, "")),
            mock.patch.object(main, "_market_fetch_ohlc", fake_ohlc),
        ):
            html = main._render_indicatorforge_preview_html(
                timeframe="1h",
                symbols="TQQQ",
                rules_json='[{"kind":"ma","params":{"length":20}}]',
                broker_hint="robinhood",
                include_extended_hours_data=False,
                use_current_candle=False,
            )

        self.assertIn(">41.00</td>", html)

    def test_indicatorforge_preview_accepts_english_ichimoku_names(self):
        def fake_ohlc(symbol, timeframe, broker_hint="robinhood", *, min_candles=0, include_extended=False):
            rows = _ohlc_rows(max(80, int(min_candles or 0)))
            opens, highs, lows, closes = main._market_extract_ohlc(rows)
            return opens, highs, lows, closes, rows, "regular"

        rules_json = """
        [{
          "kind": "ichimoku",
          "params": {
            "conversion_line_length": 3,
            "base_line_length": 5,
            "leading_span_b_length": 8,
            "lagging_line_displacement": 5,
            "buy_conditions": ["conversion_line_above_base_line"],
            "sell_conditions": ["lagging_line_below_price"],
            "base_line_bounce_tolerance_pct": 0.25
          }
        }]
        """

        with (
            mock.patch.object(main, "_ensure_robinhood_markets_session", return_value=(True, "")),
            mock.patch.object(main, "_market_fetch_ohlc", fake_ohlc),
        ):
            html = main._render_indicatorforge_preview_html(
                timeframe="1h",
                symbols="TQQQ",
                rules_json=rules_json,
                broker_hint="robinhood",
                include_extended_hours_data=False,
            )

        self.assertIn("Conversion/Base/Leading B 3/5/8", html)
        self.assertIn("Conversion Line above Base Line", html)
        self.assertIn("Lagging Line below price", html)

    def test_ichimoku_price_cloud_conditions_use_live_displaced_cloud(self):
        state = {
            "close_now": 100.0,
            "close_prev": 100.0,
            "tenkan": 101.0,
            "tenkan_prev": 101.0,
            "tenkan_prev2": 101.0,
            "kijun": 99.0,
            "kijun_prev": 99.0,
            "kijun_prev2": 99.0,
            "span_a": 99.0,
            "span_b": 101.0,
            "span_a_prev": 98.0,
            "span_b_prev": 102.0,
            "future_span_a": 130.0,
            "future_span_b": 125.0,
            "future_span_a_prev": 120.0,
            "future_span_b_prev": 130.0,
            "cloud_top": 101.0,
            "cloud_bottom": 99.0,
            "cloud_top_prev": 102.0,
            "cloud_bottom_prev": 98.0,
            "cloud_thickness_pct": 2.0,
            "cloud_thickness_prev_pct": 4.0,
            "chikou_ref_price": 99.0,
            "chikou_prev_ref_price": 99.0,
            "chikou_cloud_top": 101.0,
            "chikou_cloud_bottom": 99.0,
            "bars_since_cross_up": None,
            "bars_since_cross_down": None,
        }

        self.assertTrue(
            main._ichimoku_condition_hit(
                "price_inside_cloud",
                state=state,
                cloud_thickness_threshold_pct=1.0,
                kijun_bounce_tolerance_pct=0.35,
                delayed_cross_lookback=3,
            )
        )
        self.assertFalse(
            main._ichimoku_condition_hit(
                "price_above_cloud",
                state=state,
                cloud_thickness_threshold_pct=1.0,
                kijun_bounce_tolerance_pct=0.35,
                delayed_cross_lookback=3,
            )
        )
        self.assertTrue(
            main._ichimoku_condition_hit(
                "cloud_bearish",
                state=state,
                cloud_thickness_threshold_pct=1.0,
                kijun_bounce_tolerance_pct=0.35,
                delayed_cross_lookback=3,
            )
        )
        self.assertTrue(
            main._ichimoku_condition_hit(
                "future_twist_bullish",
                state=state,
                cloud_thickness_threshold_pct=1.0,
                kijun_bounce_tolerance_pct=0.35,
                delayed_cross_lookback=3,
            )
        )

    def test_ichimoku_chart_series_projects_cloud_forward(self):
        prices = [float(i + 1) for i in range(80)]

        aligned = main._market_ichimoku_series(
            prices,
            tenkan_length=3,
            kijun_length=5,
            senkou_b_length=8,
            displacement=5,
        )
        projected = main._market_ichimoku_series(
            prices,
            tenkan_length=3,
            kijun_length=5,
            senkou_b_length=8,
            displacement=5,
            forward_projected=True,
        )

        self.assertEqual(len(aligned["span_a"]), len(prices))
        self.assertEqual(len(projected["span_a"]), len(prices) + 5)
        self.assertIsNotNone(projected["span_a"][-1])
        self.assertIsNotNone(projected["span_b"][-1])

    def test_indicator_override_does_not_bypass_ichimoku_hold(self):
        checks = [
            {
                "name": "RSI",
                "_rule_kind": "rsi",
                "_rule_params": {
                    "signal_override_enabled": 1,
                    "signal_override_scope": "both",
                    "signal_override_targets": ["ichi-rule"],
                },
                "buy_ok": True,
                "sell_ok": False,
                "buy_ignored": False,
                "sell_ignored": False,
                "rsi_buy_signal": True,
                "rsi_sell_signal": False,
            },
            {
                "name": "Ichimoku",
                "_rule_kind": "ichimoku",
                "_rule_id": "ichi-rule",
                "buy_ok": False,
                "sell_ok": False,
                "buy_ignored": False,
                "sell_ignored": False,
                "detail": "RSI override(both)->BUY by RSI",
            },
        ]

        main._apply_indicator_signal_overrides(checks)

        self.assertFalse(checks[1].get("_override_applied", False))
        self.assertFalse(checks[1]["buy_ok"])
        self.assertIsNone(main._indicator_override_meta(checks[1]))
        self.assertEqual(main._indicator_signal_from_checks_for_backtest(checks), "HOLD")

    def test_indicator_override_still_applies_to_non_ichimoku_targets(self):
        checks = [
            {
                "name": "RSI",
                "_rule_kind": "rsi",
                "_rule_params": {
                    "signal_override_enabled": 1,
                    "signal_override_scope": "both",
                    "signal_override_targets": ["ma-rule"],
                },
                "buy_ok": True,
                "sell_ok": False,
                "buy_ignored": False,
                "sell_ignored": False,
                "rsi_buy_signal": True,
                "rsi_sell_signal": False,
            },
            {
                "name": "MA Guard",
                "_rule_kind": "ma",
                "_rule_id": "ma-rule",
                "buy_ok": False,
                "sell_ok": False,
                "buy_ignored": False,
                "sell_ignored": False,
                "detail": "",
            },
        ]

        main._apply_indicator_signal_overrides(checks)

        self.assertTrue(checks[1].get("_override_applied", False))
        self.assertTrue(checks[1]["buy_ok"])
        self.assertEqual(main._indicator_signal_from_checks_for_backtest(checks), "BUY")

    def test_market_chart_reserves_right_buffer_for_forward_indicators(self):
        prices = [float(i + 1) for i in range(80)]

        svg = main._market_chart_svg(
            closes=prices,
            ma_lengths=[],
            ema_lengths=[],
            macd_configs=[],
            bb_configs=[],
            ttm_configs=[],
            roc_lengths=[],
            sar_configs=[],
            heikin_ashi_mode=False,
            required_points=80,
            show_price=True,
            show_rsi=False,
            show_drsi=False,
            d_ma_lengths=[],
            d_ema_lengths=[],
            ichimoku_configs=[(3, 5, 8, 5)],
        )

        price_path = re.search(r"<path d='([^']+)' stroke='#f8fafc' stroke-width='1.8'", svg)
        span_a_path = re.search(r"<path d='([^']+)' stroke='#22c55e'", svg)

        self.assertIsNotNone(price_path)
        self.assertIsNotNone(span_a_path)
        self.assertNotIn("520.00", price_path.group(1))
        self.assertIn("520.00", span_a_path.group(1))


if __name__ == "__main__":
    unittest.main()

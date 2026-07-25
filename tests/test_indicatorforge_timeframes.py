import json
import unittest

import app.main as main


class IndicatorForgeTimeframeTests(unittest.TestCase):
    def test_rule_payload_preserves_and_defaults_timeframes(self):
        rules = main._normalize_indicator_rules_payload(
            json.dumps(
                [
                    {"name": "Fast MA", "kind": "ma", "timeframe": "5m", "params": {"length": 2}},
                    {"name": "Hourly RSI", "kind": "rsi", "params": {"timeframe": "1hour"}},
                    {"name": "Default MA", "kind": "ma", "params": {"length": 3}},
                ]
            ),
            default_timeframe="10m",
        )

        self.assertEqual([rule["timeframe"] for rule in rules], ["5m", "1h", "10m"])
        self.assertEqual(list(main._rules_by_timeframe(rules, "10m").keys()), ["5m", "1h", "10m"])

    def test_rule_checks_use_each_rules_own_timeframe(self):
        rules = [
            {
                "name": "Fast 5m",
                "kind": "ma",
                "timeframe": "5m",
                "params": {"length": 2, "buy_relation": "above", "sell_relation": "below"},
            },
            {
                "name": "Slow 1h",
                "kind": "ma",
                "timeframe": "1h",
                "params": {"length": 2, "buy_relation": "above", "sell_relation": "below"},
            },
        ]
        ohlc_by_tf = {
            "5m": ([10.0, 10.0, 10.0], [10.0, 10.0, 12.0], [10.0, 10.0, 10.0], [10.0, 10.0, 12.0]),
            "1h": ([20.0, 20.0, 20.0], [20.0, 20.0, 20.0], [20.0, 20.0, 18.0], [20.0, 20.0, 18.0]),
        }

        checks = main._build_indicator_rule_checks_by_timeframe(
            rules,
            ohlc_by_tf,
            default_timeframe="10m",
            apply_overrides=False,
        )

        self.assertEqual([check["_timeframe"] for check in checks], ["5m", "1h"])
        self.assertTrue(checks[0]["buy_ok"])
        self.assertFalse(checks[0]["sell_ok"])
        self.assertFalse(checks[1]["buy_ok"])
        self.assertTrue(checks[1]["sell_ok"])

    def test_missing_extended_notice_is_only_for_intraday_extended_requests(self):
        self.assertFalse(
            main._market_should_note_missing_extended_candles(
                timeframe="1d",
                extended_enabled=True,
                requested_bounds="regular",
            )
        )
        self.assertFalse(
            main._market_should_note_missing_extended_candles(
                timeframe="1d",
                extended_enabled=True,
                requested_bounds="extended",
            )
        )
        self.assertFalse(
            main._market_should_note_missing_extended_candles(
                timeframe="5m",
                extended_enabled=False,
                requested_bounds="extended",
            )
        )
        self.assertTrue(
            main._market_should_note_missing_extended_candles(
                timeframe="5m",
                extended_enabled=True,
                requested_bounds="extended",
            )
        )


if __name__ == "__main__":
    unittest.main()

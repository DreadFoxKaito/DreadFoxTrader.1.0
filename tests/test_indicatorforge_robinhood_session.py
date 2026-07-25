import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest


def load_indicatorforge_module():
    path = Path(__file__).resolve().parents[1] / "app" / "scripts" / "IndicatorForge.Robinhood.py"
    spec = importlib.util.spec_from_file_location("indicatorforge_robinhood_for_tests", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class IndicatorForgeRobinhoodSessionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_indicatorforge_module()

    def market_hours(self):
        return {
            "is_open": False,
            "extended_opens_at": "2026-06-18T08:00:00Z",
            "opens_at": "2026-06-18T13:30:00Z",
            "closes_at": "2026-06-18T20:00:00Z",
            "extended_closes_at": "2026-06-19T00:00:00Z",
        }

    def test_premarket_uses_extended_window_even_when_is_open_false(self):
        now = datetime(2026, 6, 18, 11, 0, tzinfo=timezone.utc)
        self.assertEqual(self.mod._classify_market_hours(self.market_hours(), now), "premarket")

    def test_regular_session_uses_regular_window(self):
        now = datetime(2026, 6, 18, 15, 0, tzinfo=timezone.utc)
        self.assertEqual(self.mod._classify_market_hours(self.market_hours(), now), "regular")

    def test_after_hours_uses_extended_window_even_when_is_open_false(self):
        now = datetime(2026, 6, 18, 22, 0, tzinfo=timezone.utc)
        self.assertEqual(self.mod._classify_market_hours(self.market_hours(), now), "after_hours")

    def test_premarket_falls_back_when_extended_bounds_missing(self):
        hours = self.market_hours()
        hours.pop("extended_opens_at")
        hours.pop("extended_closes_at")
        now = datetime(2026, 6, 18, 11, 0, tzinfo=timezone.utc)
        self.assertEqual(self.mod._classify_market_hours(hours, now), "premarket")

    def test_after_hours_falls_back_when_extended_bounds_missing(self):
        hours = self.market_hours()
        hours.pop("extended_opens_at")
        hours.pop("extended_closes_at")
        now = datetime(2026, 6, 18, 22, 0, tzinfo=timezone.utc)
        self.assertEqual(self.mod._classify_market_hours(hours, now), "after_hours")

    def test_outside_extended_window_is_closed(self):
        now = datetime(2026, 6, 18, 7, 0, tzinfo=timezone.utc)
        self.assertEqual(self.mod._classify_market_hours(self.market_hours(), now), "closed")

    def test_closed_market_state_uses_enabled_after_hours_window(self):
        now = datetime(2026, 6, 18, 22, 0, tzinfo=timezone.utc)  # 6:00 PM ET Thursday.
        self.assertEqual(
            self.mod._execution_state_from_market(
                "closed",
                allow_extended_hours_orders=True,
                allow_seamless_overnight_orders=False,
                now_dt=now,
            ),
            "extended",
        )

    def test_closed_market_state_keeps_after_hours_closed_when_disabled(self):
        now = datetime(2026, 6, 18, 22, 0, tzinfo=timezone.utc)
        self.assertEqual(
            self.mod._execution_state_from_market(
                "closed",
                allow_extended_hours_orders=False,
                allow_seamless_overnight_orders=True,
                now_dt=now,
            ),
            "closed",
        )

    def test_overnight_flag_enables_gap_between_afterhours_and_premarket(self):
        now = datetime(2026, 6, 19, 5, 0, tzinfo=timezone.utc)  # 1:00 AM ET Friday.
        self.assertEqual(
            self.mod._execution_state_from_market(
                "closed",
                allow_extended_hours_orders=True,
                allow_seamless_overnight_orders=True,
                now_dt=now,
            ),
            "overnight",
        )

    def test_early_premarket_routes_all_day_until_robinhood_extended_opens(self):
        now = datetime(2026, 6, 18, 10, 0, tzinfo=timezone.utc)  # 6:00 AM ET Thursday.
        self.assertEqual(
            self.mod._execution_state_from_market(
                "premarket",
                allow_extended_hours_orders=True,
                allow_seamless_overnight_orders=True,
                now_dt=now,
            ),
            "overnight",
        )

    def test_early_premarket_stays_closed_without_all_day_orders(self):
        now = datetime(2026, 6, 18, 10, 0, tzinfo=timezone.utc)  # 6:00 AM ET Thursday.
        self.assertEqual(
            self.mod._execution_state_from_market(
                "premarket",
                allow_extended_hours_orders=True,
                allow_seamless_overnight_orders=False,
                now_dt=now,
            ),
            "closed",
        )

    def test_robinhood_extended_orders_start_at_seven_am_et(self):
        now = datetime(2026, 6, 18, 11, 0, tzinfo=timezone.utc)  # 7:00 AM ET Thursday.
        self.assertEqual(
            self.mod._execution_state_from_market(
                "premarket",
                allow_extended_hours_orders=True,
                allow_seamless_overnight_orders=True,
                now_dt=now,
            ),
            "extended",
        )

    def test_overnight_flag_disabled_keeps_gap_closed(self):
        now = datetime(2026, 6, 19, 5, 0, tzinfo=timezone.utc)
        self.assertEqual(
            self.mod._execution_state_from_market(
                "closed",
                allow_extended_hours_orders=True,
                allow_seamless_overnight_orders=False,
                now_dt=now,
            ),
            "closed",
        )

    def test_weekend_gap_remains_closed(self):
        now = datetime(2026, 6, 20, 2, 0, tzinfo=timezone.utc)  # Friday 10:00 PM ET.
        self.assertFalse(self.mod._is_overnight_et_window(now))

    def test_overnight_routes_all_day_hours(self):
        self.assertTrue(self.mod._order_extended_hours_for_state("overnight"))
        self.assertEqual(self.mod._order_market_hours_for_state("overnight"), "all_day_hours")

    def test_sar_bearish_reversal_uses_trend_direction_not_numeric_jump(self):
        closes = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 8.0]
        highs = [10.2, 11.2, 12.2, 13.2, 14.2, 15.2, 9.0]
        lows = [9.8, 10.8, 11.8, 12.8, 13.8, 14.8, 7.0]
        rule = {
            "name": "Parabolic SAR",
            "kind": "sar",
            "params": {
                "step": 0.02,
                "max_step": 0.2,
                "buy_condition": "sar_rising",
                "sell_condition": "sar_falling",
            },
        }

        out = self.mod._eval_rule(rule, closes, closes[-1], highs=highs, lows=lows)

        self.assertFalse(out["buy_ok"])
        self.assertTrue(out["sell_ok"])
        self.assertIn("trend=down", out["value"])

    def test_sar_bullish_reversal_uses_trend_direction_not_numeric_drop(self):
        closes = [15.0, 14.0, 13.0, 12.0, 11.0, 10.0, 16.0]
        highs = [16.0, 15.0, 14.0, 13.0, 12.0, 11.0, 17.0]
        lows = [15.0, 14.0, 13.0, 12.0, 11.0, 10.0, 16.0]
        rule = {
            "name": "Parabolic SAR",
            "kind": "sar",
            "params": {
                "step": 0.02,
                "max_step": 0.2,
                "buy_condition": "sar_rising",
                "sell_condition": "sar_falling",
            },
        }

        out = self.mod._eval_rule(rule, closes, closes[-1], highs=highs, lows=lows)

        self.assertTrue(out["buy_ok"])
        self.assertFalse(out["sell_ok"])
        self.assertIn("trend=up", out["value"])

    def test_indicatorforge_chart_payload_preserves_ohlc_for_sar(self):
        closes = [10.0, 11.0, 12.0, 13.0]
        opens = [9.9, 10.8, 11.8, 12.8]
        highs = [10.2, 11.2, 12.2, 13.2]
        lows = [9.8, 10.7, 11.7, 12.7]

        chart = self.mod._build_chart_series(closes, opens=opens, highs=highs, lows=lows)

        self.assertEqual(chart["price"], closes)
        self.assertEqual(chart["open"], opens)
        self.assertEqual(chart["high"], highs)
        self.assertEqual(chart["low"], lows)

    def test_price_prefers_non_regular_trade_when_extended_requested(self):
        quote = {
            "last_trade_price": "77.210000",
            "last_extended_hours_trade_price": "77.200000",
            "last_non_reg_trade_price": "76.750000",
        }
        self.assertEqual(self.mod._price_from_quote(quote, prefer_extended=True), 76.75)
        self.assertEqual(self.mod._price_from_quote(quote, prefer_extended=False), 77.21)

    def test_extracts_bonfire_live_quote_payload(self):
        payload = {
            "chart_section": {
                "quote": {
                    "last_non_reg_trade_price": "76.750000",
                    "updated_at": "2026-06-30T02:16:58+00:00",
                }
            }
        }
        quote = self.mod._extract_live_quote(payload)
        self.assertEqual(quote["last_non_reg_trade_price"], "76.750000")
        self.assertEqual(quote["updated_at"], "2026-06-30T02:16:58+00:00")

    def test_merge_live_quote_overrides_stale_standard_quote(self):
        base = {
            "symbol": "TQQQ",
            "last_extended_hours_trade_price": "77.200000",
            "updated_at": "2026-06-30T00:00:00Z",
        }
        live = {
            "last_extended_hours_trade_price": "76.750000",
            "last_non_reg_trade_price": "76.750000",
            "updated_at": "2026-06-30T02:16:58+00:00",
        }
        merged = self.mod._merge_quote_data(base, live, source="robinhood_bonfire_live")
        self.assertEqual(merged["last_non_reg_trade_price"], "76.750000")
        self.assertEqual(merged["last_extended_hours_trade_price"], "76.750000")
        self.assertEqual(merged["_quote_source"], "robinhood_bonfire_live")
        self.assertEqual(merged["_quote_updated_at"], "2026-06-30T02:16:58+00:00")

    def test_quotes_map_uses_live_overnight_quote_when_enabled(self):
        original_safe_stock_quote = self.mod.safe_stock_quote
        original_safe_live_overnight_quote = self.mod.safe_live_overnight_quote
        try:
            self.mod.safe_stock_quote = lambda symbol, retries=2, backoff=0.5: {
                "symbol": symbol,
                "instrument_id": "instrument-1",
                "last_extended_hours_trade_price": "77.200000",
            }
            self.mod.safe_live_overnight_quote = lambda symbol, base_quote=None, retries=1, backoff=0.25: {
                **dict(base_quote or {}),
                "last_non_reg_trade_price": "76.750000",
                "_quote_source": "robinhood_bonfire_live",
            }
            quotes = self.mod.get_quotes_map(["TQQQ"], prefer_live_overnight=True)
        finally:
            self.mod.safe_stock_quote = original_safe_stock_quote
            self.mod.safe_live_overnight_quote = original_safe_live_overnight_quote
        self.assertEqual(quotes["TQQQ"]["last_non_reg_trade_price"], "76.750000")
        self.assertEqual(quotes["TQQQ"]["_quote_source"], "robinhood_bonfire_live")

    def test_portfolio_cash_source_normalization(self):
        self.assertEqual(self.mod._normalize_portfolio_cash_source(None), "buying_power")
        self.assertEqual(self.mod._normalize_portfolio_cash_source("buying power"), "buying_power")
        self.assertEqual(self.mod._normalize_portfolio_cash_source("available_cash"), "cash")
        self.assertEqual(self.mod._normalize_portfolio_cash_source("cash position"), "cash")

    def test_overnight_history_builds_timeframe_ohlc_bucket(self):
        state = {"version": self.mod.OVERNIGHT_HISTORY_STATE_VERSION, "symbols": {}}
        now = datetime(2026, 7, 6, 1, 46, tzinfo=timezone.utc)
        first = self.mod.record_overnight_price_sample(
            state=state,
            symbol="TQQQ",
            timeframe_key="10m",
            price=100.0,
            quote_updated_at=now.isoformat(),
            now_dt=now,
        )
        second = self.mod.record_overnight_price_sample(
            state=state,
            symbol="TQQQ",
            timeframe_key="10m",
            price=101.25,
            quote_updated_at=(now + timedelta(minutes=1)).isoformat(),
            now_dt=now + timedelta(minutes=1),
        )

        rows = state["symbols"]["TQQQ"]["10m"]
        self.assertTrue(first["recorded"])
        self.assertTrue(second["recorded"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["begins_at"], "2026-07-06T01:40:00Z")
        self.assertEqual(rows[0]["open_price"], 100.0)
        self.assertEqual(rows[0]["high_price"], 101.25)
        self.assertEqual(rows[0]["low_price"], 100.0)
        self.assertEqual(rows[0]["close_price"], 101.25)
        self.assertEqual(rows[0]["sample_count"], 2)

    def test_overnight_history_skips_stale_quote(self):
        state = {"version": self.mod.OVERNIGHT_HISTORY_STATE_VERSION, "symbols": {}}
        now = datetime(2026, 7, 6, 1, 46, tzinfo=timezone.utc)
        meta = self.mod.record_overnight_price_sample(
            state=state,
            symbol="TQQQ",
            timeframe_key="10m",
            price=100.0,
            quote_updated_at=(now - timedelta(hours=2)).isoformat(),
            now_dt=now,
        )

        self.assertFalse(meta["recorded"])
        self.assertTrue(meta["quote_stale"])
        self.assertEqual(meta["skip_reason"], "quote stale")
        self.assertEqual(state["symbols"], {})

    def test_overnight_history_merges_after_stale_broker_history(self):
        state = {"version": self.mod.OVERNIGHT_HISTORY_STATE_VERSION, "symbols": {}}
        now = datetime(2026, 7, 6, 1, 46, tzinfo=timezone.utc)
        self.mod.record_overnight_price_sample(
            state=state,
            symbol="TQQQ",
            timeframe_key="10m",
            price=101.25,
            quote_updated_at=now.isoformat(),
            now_dt=now,
        )
        broker_rows = [
            {
                "begins_at": "2026-07-02T23:50:00Z",
                "open_price": 99.0,
                "high_price": 99.5,
                "low_price": 98.75,
                "close_price": 99.25,
            }
        ]

        merged, count, latest_ts = self.mod._merge_overnight_synthetic_rows(
            broker_rows,
            state,
            symbol="TQQQ",
            timeframe_key="10m",
        )

        self.assertEqual(count, 1)
        self.assertEqual(latest_ts, "2026-07-06T01:40:00Z")
        self.assertEqual(len(merged), 2)
        self.assertEqual(float(merged[-1]["close_price"]), 101.25)


if __name__ == "__main__":
    unittest.main()

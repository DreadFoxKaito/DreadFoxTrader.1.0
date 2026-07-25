import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch


class Recorder:
    def __init__(self, return_value):
        self.return_value = return_value
        self.calls = []

    @property
    def call_count(self):
        return len(self.calls)

    def assert_called_once(self):
        assert len(self.calls) == 1, self.calls

    def assert_not_called(self):
        assert not self.calls, self.calls


def stock_market_recorder(return_value):
    rec = Recorder(return_value)
    def fn(symbol, quantity, account_number=None, timeInForce="gtc", extendedHours=False, jsonify=True):
        rec.calls.append(dict(symbol=symbol, quantity=quantity, account_number=account_number, timeInForce=timeInForce, extendedHours=extendedHours, jsonify=jsonify))
        return rec.return_value
    fn.recorder = rec
    return fn


def stock_limit_recorder(return_value):
    rec = Recorder(return_value)
    def fn(symbol, quantity, limitPrice, account_number=None, timeInForce="gtc", extendedHours=False, jsonify=True):
        rec.calls.append(dict(symbol=symbol, quantity=quantity, limitPrice=limitPrice, account_number=account_number, timeInForce=timeInForce, extendedHours=extendedHours, jsonify=jsonify))
        return rec.return_value
    fn.recorder = rec
    return fn


def stock_generic_order_recorder(return_value):
    rec = Recorder(return_value)
    def fn(
        symbol,
        quantity,
        side,
        limitPrice=None,
        stopPrice=None,
        account_number=None,
        timeInForce="gtc",
        extendedHours=False,
        jsonify=True,
        market_hours="regular_hours",
    ):
        rec.calls.append(
            dict(
                symbol=symbol,
                quantity=quantity,
                side=side,
                limitPrice=limitPrice,
                stopPrice=stopPrice,
                account_number=account_number,
                timeInForce=timeInForce,
                extendedHours=extendedHours,
                jsonify=jsonify,
                market_hours=market_hours,
            )
        )
        return rec.return_value
    fn.recorder = rec
    return fn


def stock_trailing_recorder(return_value):
    rec = Recorder(return_value)
    def fn(symbol, quantity, side, trailAmount, trailType="percentage", account_number=None, timeInForce="gtc", extendedHours=False, jsonify=True):
        rec.calls.append(dict(symbol=symbol, quantity=quantity, side=side, trailAmount=trailAmount, trailType=trailType, account_number=account_number, timeInForce=timeInForce, extendedHours=extendedHours, jsonify=jsonify))
        return rec.return_value
    fn.recorder = rec
    return fn

from app.brokers import robin_stocks_adapter as adapter


def ok(order_id="ord-1", state="queued"):
    return {"id": order_id, "state": state}


class RobinStocksAdapterTests(unittest.TestCase):
    def setUp(self):
        adapter.set_order_kill_switch(False)
        os.environ.pop("ROBIN_STOCKS_ADAPTER_KILL_SWITCH", None)

    def tearDown(self):
        adapter.set_order_kill_switch(False)
        os.environ.pop("ROBIN_STOCKS_ADAPTER_KILL_SWITCH", None)

    def fake_rh(self, **funcs):
        orders = SimpleNamespace(**funcs)
        stocks = SimpleNamespace()
        crypto = SimpleNamespace()
        return SimpleNamespace(orders=orders, stocks=stocks, crypto=crypto)

    def test_stock_market_selects_documented_wrapper(self):
        fn = stock_market_recorder(ok())
        fake = self.fake_rh(order_buy_market=fn)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(symbol="AAPL", side="buy", order_type="market", quantity=2, account_number="acct-123")
        self.assertTrue(result.accepted)
        self.assertEqual(result.robin_stocks_function, "orders.order_buy_market")
        self.assertEqual(fn.recorder.calls, [dict(symbol="AAPL", quantity=2, account_number="acct-123", timeInForce="gtc", extendedHours=False, jsonify=True)])
        self.assertEqual(result.sanitized_payload["account_number"], "***REDACTED***")

    def test_stock_sell_trailing_stop_uses_legacy_compatible_generic_wrapper(self):
        generic = stock_trailing_recorder(ok())
        side_specific = Mock(return_value=ok("bad"))
        fake = self.fake_rh(order_sell_trailing_stop=side_specific, order_trailing_stop=generic)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(
                symbol="MSFT",
                side="sell",
                order_type="trailing_stop",
                quantity=1,
                trailAmount=0.25,
                trailType="amount",
                account_number="acct-123",
            )
        self.assertTrue(result.accepted)
        self.assertEqual(result.robin_stocks_function, "orders.order_trailing_stop")
        generic.recorder.assert_called_once()
        side_specific.assert_not_called()
        self.assertEqual(generic.recorder.calls[0]["side"], "sell")
        self.assertEqual(generic.recorder.calls[0]["trailAmount"], 0.25)
        self.assertEqual(generic.recorder.calls[0]["trailType"], "amount")
        self.assertEqual(generic.recorder.calls[0]["account_number"], "acct-123")
        self.assertEqual(
            result.sanitized_payload,
            {
                "symbol": "MSFT",
                "quantity": 1,
                "trailAmount": 0.25,
                "trailType": "amount",
                "timeInForce": "gtc",
                "side": "sell",
                "account_number": "***REDACTED***",
            },
        )

    def test_stock_buy_trailing_stop_uses_legacy_compatible_generic_wrapper(self):
        generic = stock_trailing_recorder(ok())
        side_specific = Mock(return_value=ok("bad"))
        fake = self.fake_rh(order_buy_trailing_stop=side_specific, order_trailing_stop=generic)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(
                symbol="MSFT",
                side="buy",
                order_type="trailing_stop",
                quantity=1,
                trailAmount=0.25,
                trailType="amount",
                account_number="acct-123",
            )
        self.assertTrue(result.accepted)
        self.assertEqual(result.robin_stocks_function, "orders.order_trailing_stop")
        generic.recorder.assert_called_once()
        side_specific.assert_not_called()
        self.assertEqual(generic.recorder.calls[0]["side"], "buy")
        self.assertEqual(generic.recorder.calls[0]["trailAmount"], 0.25)
        self.assertEqual(generic.recorder.calls[0]["trailType"], "amount")
        self.assertEqual(generic.recorder.calls[0]["account_number"], "acct-123")
        self.assertEqual(
            result.sanitized_payload,
            {
                "symbol": "MSFT",
                "quantity": 1,
                "trailAmount": 0.25,
                "trailType": "amount",
                "timeInForce": "gtc",
                "side": "buy",
                "account_number": "***REDACTED***",
            },
        )

    def test_missing_trailing_stop_wrapper_blocks_before_submission(self):
        fake = self.fake_rh()
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(symbol="MSFT", side="sell", order_type="trailing_stop", quantity=1, trailAmount=0.25)
        self.assertTrue(result.blocked)
        self.assertFalse(result.submitted)
        self.assertEqual(result.reason, "TRAILING_STOP_WRAPPER_UNAVAILABLE")

    def test_premarket_limit_buy_reaches_adapter_with_extended_hours(self):
        fn = stock_generic_order_recorder(ok())
        side_wrapper = stock_limit_recorder(ok("bad"))
        fake = self.fake_rh(order=fn, order_buy_limit=side_wrapper)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(
                symbol="AAPL",
                side="buy",
                order_type="limit",
                quantity=1,
                limitPrice=100,
                market_session="premarket",
                extendedHours=True,
            )
        self.assertTrue(result.accepted)
        self.assertEqual(result.robin_stocks_function, "orders.order")
        side_wrapper.recorder.assert_not_called()
        self.assertTrue(fn.recorder.calls[0]["extendedHours"])
        self.assertEqual(fn.recorder.calls[0]["market_hours"], "extended_hours")
        self.assertEqual(fn.recorder.calls[0]["side"], "buy")
        self.assertEqual(fn.recorder.calls[0]["limitPrice"], 100.0)

    def test_premarket_limit_sell_reaches_adapter_with_extended_hours(self):
        fn = stock_generic_order_recorder(ok())
        side_wrapper = stock_limit_recorder(ok("bad"))
        fake = self.fake_rh(order=fn, order_sell_limit=side_wrapper)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(
                symbol="AAPL",
                side="sell",
                order_type="limit",
                quantity=1,
                limitPrice=100,
                market_session="premarket",
                extendedHours=True,
            )
        self.assertTrue(result.accepted)
        self.assertEqual(result.robin_stocks_function, "orders.order")
        side_wrapper.recorder.assert_not_called()
        self.assertTrue(fn.recorder.calls[0]["extendedHours"])
        self.assertEqual(fn.recorder.calls[0]["market_hours"], "extended_hours")
        self.assertEqual(fn.recorder.calls[0]["side"], "sell")
        self.assertEqual(fn.recorder.calls[0]["limitPrice"], 100.0)

    def test_premarket_market_order_is_blocked(self):
        fn = stock_market_recorder(ok())
        fake = self.fake_rh(order_buy_market=fn)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(
                symbol="AAPL",
                side="buy",
                order_type="market",
                quantity=1,
                market_session="premarket",
            )
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason, "MARKET_ORDER_NOT_SUPPORTED_FOR_PREMARKET")
        fn.recorder.assert_not_called()

    def test_premarket_trailing_stop_will_queue_decision(self):
        decision = adapter.validate_stock_order_session(
            market_session="premarket",
            symbol="AAPL",
            side="sell",
            order_type="trailing_stop",
            extendedHours=False,
            timeInForce="gtc",
        )
        self.assertTrue(decision.can_submit)
        self.assertFalse(decision.can_execute_now)
        self.assertTrue(decision.will_queue)
        self.assertFalse(decision.blocked)
        self.assertNotEqual(decision.reason, "MARKET_SESSION_CLOSED")

    def test_stale_closed_extended_limit_session_is_allowed_during_premarket(self):
        decision = adapter.validate_stock_order_session(
            market_session="closed",
            symbol="AAPL",
            side="buy",
            order_type="limit",
            extendedHours=True,
            timeInForce="gfd",
            market_hours="extended_hours",
            now_dt=datetime(2026, 7, 2, 12, 10, tzinfo=timezone.utc),
        )
        self.assertTrue(decision.can_submit)
        self.assertTrue(decision.can_execute_now)
        self.assertFalse(decision.blocked)
        self.assertEqual(decision.reason, "STALE_CLOSED_EXTENDED_LIMIT_ALLOWED")

    def test_stale_closed_extended_limit_blocks_before_robinhood_extended_opens(self):
        decision = adapter.validate_stock_order_session(
            market_session="closed",
            symbol="AAPL",
            side="buy",
            order_type="limit",
            extendedHours=True,
            timeInForce="gfd",
            market_hours="extended_hours",
            now_dt=datetime(2026, 7, 2, 10, 30, tzinfo=timezone.utc),
        )
        self.assertTrue(decision.blocked)
        self.assertEqual(decision.reason, "MARKET_SESSION_CLOSED")

    def test_stale_closed_extended_limit_session_still_blocks_outside_extended_hours(self):
        decision = adapter.validate_stock_order_session(
            market_session="closed",
            symbol="AAPL",
            side="buy",
            order_type="limit",
            extendedHours=True,
            timeInForce="gfd",
            market_hours="extended_hours",
            now_dt=datetime(2026, 7, 2, 5, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(decision.blocked)
        self.assertEqual(decision.reason, "MARKET_SESSION_CLOSED")

    def test_place_stock_order_allows_stale_closed_extended_limit_when_clock_is_extended(self):
        fn = stock_generic_order_recorder(ok())
        fake = self.fake_rh(order=fn)
        with (
            patch.object(adapter, "rh", fake),
            patch.object(adapter, "_IMPORT_ERR", None),
            patch.object(adapter, "_common_extended_hours_label", return_value="premarket"),
        ):
            result = adapter.place_stock_order(
                symbol="AAPL",
                side="buy",
                order_type="limit",
                quantity=1,
                limitPrice=100,
                market_session="closed",
                market_hours="extended_hours",
                extendedHours=True,
            )
        self.assertTrue(result.accepted)
        self.assertEqual(result.robin_stocks_function, "orders.order")
        self.assertEqual(fn.recorder.calls[0]["market_hours"], "extended_hours")
        self.assertTrue(fn.recorder.calls[0]["extendedHours"])

    def test_blocked_session_decision_is_sanitized_to_dict(self):
        result = adapter.place_stock_order(
            symbol="AAPL",
            side="buy",
            order_type="market",
            quantity=1,
            market_session="premarket",
        )
        self.assertTrue(result.blocked)
        self.assertIsInstance(result.sanitized_payload.get("session_decision"), dict)
        self.assertEqual(result.sanitized_payload["session_decision"]["detected_session"], "premarket")

    def test_unsupported_order_type_blocks_before_submission(self):
        fn = stock_market_recorder(ok())
        fake = self.fake_rh(order_buy_market=fn)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(symbol="AAPL", side="buy", order_type="oco", quantity=1)
        self.assertTrue(result.blocked)
        self.assertFalse(result.submitted)
        fn.recorder.assert_not_called()

    def test_crypto_extended_hours_is_blocked(self):
        fn = Mock(return_value=ok())
        fake = self.fake_rh(order_buy_crypto_by_price=fn)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_crypto_order(
                symbol="BTC",
                side="buy",
                order_type="market",
                amountInDollars=10,
                extendedHours=True,
            )
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason, "EXTENDED_HOURS_NOT_SUPPORTED_FOR_CRYPTO_24_7")
        fn.assert_not_called()

    def test_crypto_trailing_stop_is_blocked(self):
        fake = self.fake_rh()
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_crypto_order(symbol="BTC", side="sell", order_type="trailing_stop", amountInDollars=10)
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason, "TRAILING_STOP_UNSUPPORTED_BY_ROBIN_STOCKS")

    def test_stock_all_day_market_hours_requires_limit_order(self):
        fn = stock_market_recorder(ok())
        fake = self.fake_rh(order_buy_market=fn)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(
                symbol="AAPL",
                side="buy",
                order_type="market",
                quantity=1,
                market_hours="all_day_hours",
            )
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason, "EXTENDED_MARKET_HOURS_REQUIRES_LIMIT_ORDER")
        fn.recorder.assert_not_called()

    def test_stock_all_day_limit_uses_generic_order_market_hours(self):
        fn = stock_generic_order_recorder(ok())
        fake = self.fake_rh(order=fn)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_stock_order(
                symbol="AAPL",
                side="buy",
                order_type="limit",
                quantity=1,
                limitPrice=100,
                market_session="overnight",
                market_hours="all_day_hours",
            )
        self.assertTrue(result.accepted)
        self.assertEqual(result.robin_stocks_function, "orders.order")
        self.assertEqual(fn.recorder.calls[0]["market_hours"], "all_day_hours")
        self.assertTrue(fn.recorder.calls[0]["extendedHours"])
        self.assertEqual(fn.recorder.calls[0]["side"], "buy")

    def test_option_extended_hours_is_blocked(self):
        fake = self.fake_rh(order_buy_option_limit=Mock(return_value=ok()))
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            result = adapter.place_option_order(
                symbol="AAPL",
                side="buy",
                order_type="limit",
                quantity=1,
                expirationDate="2026-06-19",
                strike="200",
                optionType="call",
                positionEffect="open",
                creditOrDebit="debit",
                price=1.23,
                extendedHours=True,
            )
        self.assertTrue(result.blocked)
        self.assertEqual(result.reason, "EXTENDED_HOURS_NOT_SUPPORTED_FOR_OPTIONS_WRAPPER")

    def test_error_dict_rejected_missing_id_and_throttle_are_detected(self):
        cases = [
            ({"detail": "bad order"}, "bad order"),
            ({"state": "rejected", "id": "ord-2"}, "ROBIN_STOCKS_ORDER_REJECTED"),
            ({"state": "queued"}, "MISSING_ORDER_ID_FROM_ROBIN_STOCKS"),
            ({"detail": "429 too many requests"}, "ROBIN_STOCKS_THROTTLED: 429 too many requests"),
        ]
        for response, reason in cases:
            fn = Mock(return_value=response)
            fake = self.fake_rh(order_buy_market=fn)
            with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
                result = adapter.place_stock_order(symbol="AAPL", side="buy", order_type="market", quantity=1)
            self.assertFalse(result.accepted)
            self.assertEqual(result.reason, reason)

    def test_10m_uses_native_supported_interval_when_enough(self):
        native = [{"begins_at": f"2026-01-01T00:{i:02d}:00Z", "open_price": "1", "high_price": "1", "low_price": "1", "close_price": "1"} for i in range(3)]
        stocks = SimpleNamespace(get_stock_historicals=Mock(return_value=native))
        fake = SimpleNamespace(orders=SimpleNamespace(), stocks=stocks, crypto=SimpleNamespace())
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            rows = adapter.get_10m_stock_historicals("AAPL", min_candles=3)
        self.assertEqual(rows, native)
        stocks.get_stock_historicals.assert_called_once_with("AAPL", interval="10minute", span="week", bounds="regular", info=None)

    def test_10m_resamples_5m_and_blocks_if_still_insufficient(self):
        stocks = SimpleNamespace(get_stock_historicals=Mock(side_effect=[[], []]))
        fake = SimpleNamespace(orders=SimpleNamespace(), stocks=stocks, crypto=SimpleNamespace())
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            with self.assertRaisesRegex(RuntimeError, "INSUFFICIENT_CANDLES_FOR_10M_CALCULATION"):
                adapter.get_10m_stock_historicals("AAPL", min_candles=150)
        calls = stocks.get_stock_historicals.call_args_list
        self.assertEqual(calls[0].kwargs["interval"], "10minute")
        self.assertEqual(calls[1].kwargs["interval"], "5minute")

    def test_10m_allow_partial_returns_best_supported_partial_rows(self):
        native = [
            {"begins_at": "2026-01-01T00:00:00Z", "open_price": "1", "high_price": "1", "low_price": "1", "close_price": "1"}
        ]
        lower = [
            {"begins_at": f"2026-01-01T00:{i:02d}:00Z", "open_price": "1", "high_price": "2", "low_price": "1", "close_price": "2"}
            for i in range(0, 20, 5)
        ]
        stocks = SimpleNamespace(get_stock_historicals=Mock(side_effect=[native, lower]))
        fake = SimpleNamespace(orders=SimpleNamespace(), stocks=stocks, crypto=SimpleNamespace())
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            rows = adapter.get_10m_stock_historicals("AAPL", min_candles=150, allow_partial=True)
        self.assertEqual(len(rows), 2)

    def test_order_kill_switch_blocks_ghost_loop_submissions(self):
        fn = stock_market_recorder(ok())
        fake = self.fake_rh(order_buy_market=fn)
        with patch.object(adapter, "rh", fake), patch.object(adapter, "_IMPORT_ERR", None):
            first = adapter.place_stock_order(symbol="AAPL", side="buy", order_type="market", quantity=1)
            adapter.set_order_kill_switch(True)
            second = adapter.place_stock_order(symbol="AAPL", side="buy", order_type="market", quantity=1)
        self.assertTrue(first.accepted)
        self.assertTrue(second.blocked)
        self.assertEqual(second.reason, "ADAPTER_ORDER_KILL_SWITCH_ACTIVE")
        self.assertEqual(fn.recorder.call_count, 1)


if __name__ == "__main__":
    unittest.main()

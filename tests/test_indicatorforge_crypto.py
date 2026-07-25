import importlib.util
import json
import unittest
from pathlib import Path


def _load_crypto_module():
    script_path = (
        Path(__file__).resolve().parents[1]
        / "app"
        / "scripts"
        / "IndicatorForge.Crypto.Robinhood.py"
    )
    spec = importlib.util.spec_from_file_location("indicatorforge_crypto_robinhood", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class IndicatorForgeCryptoConsensusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = _load_crypto_module()

    def test_inline_rules_keep_heikin_ashi(self):
        rules = self.mod._normalize_inline_rules(
            json.dumps(
                [
                    {"name": "RSI Derivative", "kind": "rsi_d", "params": {}},
                    {"name": "Heikin Ashi", "kind": "heikin_ashi", "params": {"mode": "state"}},
                    {"name": "HA Alias", "kind": "ha", "params": {"mode": "state"}},
                ]
            )
        )

        self.assertEqual([rule["kind"] for rule in rules], ["rsi_d", "heikin_ashi", "heikin_ashi"])

    def test_heikin_ashi_sell_blocks_buy_consensus(self):
        ha = self.mod._eval_rule(
            {"name": "Heikin Ashi", "kind": "heikin_ashi", "params": {"mode": "state"}},
            [9.0, 8.0],
            8.0,
            opens=[10.0, 10.0],
            highs=[11.0, 11.0],
            lows=[8.0, 7.0],
        )

        self.assertFalse(ha["buy_ok"])
        self.assertTrue(ha["sell_ok"])
        checks = [
            {"name": "RSI Derivative", "_rule_kind": "rsi_d", "buy_ok": True, "sell_ok": False},
            {"name": "Ichimoku", "_rule_kind": "ichimoku", "buy_ok": True, "sell_ok": False},
            {**ha, "name": "Heikin Ashi", "_rule_kind": "heikin_ashi"},
        ]

        self.assertEqual(self.mod._strict_consensus_signal(checks), "HOLD")

    def test_all_buy_consensus_includes_heikin_ashi(self):
        ha = self.mod._eval_rule(
            {"name": "Heikin Ashi", "kind": "heikin_ashi", "params": {"mode": "state"}},
            [11.0, 12.0],
            12.0,
            opens=[10.0, 10.0],
            highs=[12.0, 12.0],
            lows=[9.0, 9.0],
        )

        self.assertTrue(ha["buy_ok"])
        self.assertFalse(ha["sell_ok"])
        checks = [
            {"name": "RSI Derivative", "_rule_kind": "rsi_d", "buy_ok": True, "sell_ok": False},
            {"name": "Ichimoku", "_rule_kind": "ichimoku", "buy_ok": True, "sell_ok": False},
            {**ha, "name": "Heikin Ashi", "_rule_kind": "heikin_ashi"},
        ]

        self.assertEqual(self.mod._strict_consensus_signal(checks), "BUY")

    def test_crypto_order_amount_minimum_allows_quarter_dollar(self):
        self.assertEqual(self.mod._normalize_order_amount_dollars(0.25), 0.25)
        self.assertEqual(self.mod._normalize_order_amount_dollars(0.259), 0.25)
        self.assertIsNone(self.mod._normalize_order_amount_dollars(0.24))

    def test_crypto_sell_amount_minimum_allows_quarter_dollar_position_value(self):
        self.assertEqual(
            self.mod._compute_sell_order_amount(pos_qty=0.5, current_price=0.50, desired_amount=0.25),
            0.25,
        )
        self.assertIsNone(
            self.mod._compute_sell_order_amount(pos_qty=0.4, current_price=0.50, desired_amount=0.25)
        )


if __name__ == "__main__":
    unittest.main()

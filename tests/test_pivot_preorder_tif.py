from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


def _source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _block_after_marker(source: str, marker: str, *, length: int = 1100) -> str:
    start = source.index(marker)
    return source[start : start + length]


def _literal_keyword(call: ast.Call, name: str) -> object:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant):
            return kw.value.value
    return None


def _calls_named(tree: ast.AST, names: set[str]) -> list[ast.Call]:
    out: list[ast.Call] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in names:
            out.append(node)
    return out


class PivotPreorderTimeInForceTests(unittest.TestCase):
    def test_robinhood_pivot_preorder_sell_uses_session_time_in_force(self):
        src = _source("app/scripts/IndicatorForge.Robinhood.py")
        block = _block_after_marker(src, "Pre-sale order -> placing limit SELL")

        self.assertIn("sell_resp = place_limit_sell(", block)
        self.assertIn("market_session=route_market_session", block)
        self.assertIn("market_hours=route_market_hours", block)
        self.assertIn("time_in_force=_preorder_time_in_force_for_state(session_state)", block)

    def test_schwab_pivot_preorder_sell_uses_session_duration(self):
        src = _source("app/scripts/IndicatorForge.Schwab.py")
        block = _block_after_marker(src, "Pre-sale order -> placing limit SELL")

        self.assertIn("sell_resp = place_limit_with_session_fallback(", block)
        self.assertIn('side="sell"', block)
        self.assertIn("session_state=session_state", block)
        self.assertIn("session_tag=session_tag", block)
        self.assertIn("duration=_preorder_duration_for_state(session_state)", block)

    def test_robinhood_preorder_time_in_force_is_day_for_detected_sessions(self):
        src = _source("app/scripts/IndicatorForge.Robinhood.py")

        self.assertIn('if session in ("regular", "extended", "premarket", "after_hours", "overnight"):', src)
        self.assertIn('return "gfd"', src)

    def test_schwab_preorder_duration_is_day_for_detected_sessions(self):
        src = _source("app/scripts/IndicatorForge.Schwab.py")

        self.assertIn('if session in ("regular", "extended", "overnight"):', src)
        self.assertIn('return "DAY"', src)

    def test_schwab_limit_sell_default_duration_is_good_till_cancel(self):
        src = _source("app/scripts/IndicatorForge.Schwab.py")

        self.assertIn(
            'def place_limit_sell(symbol: str, qty: float, session: str, price: float, *, duration: str = "GOOD_TILL_CANCEL")',
            src,
        )

    def test_robinhood_stock_and_crypto_limit_sell_calls_are_gtc(self):
        paths = [
            "app/scripts/FoxBalance.Robinhood.py",
            "app/scripts/Rokurokubi.Options.Robinhood.py",
            "app/scripts/Dreadfox.Crypto.Robinhood.py",
            "app/scripts/IndicatorForge.Crypto.Robinhood.py",
            "app/scripts/IndicatorForge.Robinhood.py",
        ]
        bad: list[str] = []
        for rel in paths:
            tree = ast.parse(_source(rel))
            for call in _calls_named(tree, {"place_stock_order", "place_crypto_order"}):
                if _literal_keyword(call, "side") != "sell" or _literal_keyword(call, "order_type") != "limit":
                    continue
                tif = _literal_keyword(call, "timeInForce")
                if tif not in (None, "gtc"):
                    bad.append(f"{rel}: line {call.lineno} timeInForce={tif!r}")

        self.assertEqual(bad, [])

    def test_schwab_limit_sell_helpers_are_good_till_cancel(self):
        paths = [
            "app/scripts/Rokurokubi.Options.Schwab.py",
            "app/scripts/Superhexagon.Schwab.py",
            "app/scripts/FoxBalance.Schwab.py",
            "app/scripts/DreadFox.Stock.Schwab.py",
            "app/scripts/EntangledTickers.Schwab.py",
            "app/scripts/IndicatorForge.Schwab.py",
        ]
        for rel in paths:
            src = _source(rel)
            block = _block_after_marker(src, "def place_limit_sell", length=350)
            self.assertIn("GOOD_TILL_CANCEL", block, rel)

        options_block = _block_after_marker(
            _source("app/scripts/Rokurokubi.Options.Schwab.py"),
            "def place_sell_to_open_limit",
            length=450,
        )
        self.assertIn("GOOD_TILL_CANCEL", options_block)


if __name__ == "__main__":
    unittest.main()

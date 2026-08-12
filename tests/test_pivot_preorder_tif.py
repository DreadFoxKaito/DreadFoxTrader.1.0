from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


def _source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _block_after_marker(source: str, marker: str, *, length: int = 1100) -> str:
    start = source.index(marker)
    return source[start : start + length]


class PivotPreorderTimeInForceTests(unittest.TestCase):
    def test_robinhood_pivot_preorder_sell_is_gtc(self):
        src = _source("app/scripts/IndicatorForge.Robinhood.py")
        block = _block_after_marker(src, "Pivot preorder -> placing limit SELL")

        self.assertIn("sell_resp = place_limit_sell(", block)
        self.assertIn('time_in_force="gtc"', block)

    def test_schwab_pivot_preorder_sell_is_good_till_cancel(self):
        src = _source("app/scripts/IndicatorForge.Schwab.py")
        block = _block_after_marker(src, "Pivot preorder -> placing limit SELL")

        self.assertIn("sell_resp = place_limit_with_session_fallback(", block)
        self.assertIn('side="sell"', block)
        self.assertIn('duration="GOOD_TILL_CANCEL"', block)

    def test_schwab_limit_order_default_duration_stays_day(self):
        src = _source("app/scripts/IndicatorForge.Schwab.py")

        self.assertIn('def place_limit_sell(symbol: str, qty: float, session: str, price: float, *, duration: str = "DAY")', src)


if __name__ == "__main__":
    unittest.main()

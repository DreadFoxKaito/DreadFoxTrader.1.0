from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import app.main as main


def _rows(count: int = 180) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for i in range(count):
        close = 100.0 + (i * 0.4)
        open_ = close - 0.2
        rows.append(
            {
                "begins_at": f"2026-01-{1 + (i // 24):02d}T{i % 24:02d}:00:00Z",
                "open_price": str(open_),
                "high_price": str(close + 1.0),
                "low_price": str(open_ - 1.0),
                "close_price": str(close),
                "volume": "1000",
                "session": "regular",
            }
        )
    return rows


class StrategyForgeUiTests(unittest.TestCase):
    def test_strategy_forge_quick_partial_runs_and_returns_table(self):
        rows = _rows()
        opens, highs, lows, closes = main._market_extract_ohlc(rows)
        with tempfile.TemporaryDirectory() as tmp:
            with (
                mock.patch.object(main, "_ensure_robinhood_markets_session", return_value=(True, "")),
                mock.patch.object(
                    main,
                    "_market_fetch_ohlc",
                    return_value=(opens, highs, lows, closes, rows, "regular"),
                ),
            ):
                html = main._render_strategy_forge_quick_html(
                    timeframe="1h",
                    symbols="TQQQ",
                    trials=2,
                    min_trades=1,
                    broker_hint="robinhood",
                    include_extended_hours_data=False,
                    db_path=Path(tmp) / "forge.sqlite3",
                )
        self.assertIn("Strategy Forge", html)
        self.assertIn("open combo evolution", html)
        self.assertIn("<table>", html)

    def test_create_and_edit_pages_include_template_free_strategy_forge(self):
        root = Path(__file__).resolve().parents[1]
        new_html = (root / "app" / "templates" / "algo_new.html").read_text(encoding="utf-8")
        edit_html = (root / "app" / "templates" / "algo_edit.html").read_text(encoding="utf-8")
        self.assertIn("/partials/strategy_forge_quick", new_html)
        self.assertIn("/partials/strategy_forge_quick", edit_html)
        self.assertNotIn("if_forge_template", new_html)
        self.assertNotIn("if_forge_template", edit_html)
        self.assertNotIn("generic_forge_template", new_html)
        self.assertNotIn("generic_forge_template", edit_html)
        self.assertIn("Open combo evolution", new_html)
        self.assertIn("Open combo evolution", edit_html)
        self.assertIn("if_forge_run_btn", new_html)
        self.assertIn("if_forge_run_btn", edit_html)
        self.assertIn("generic_forge_run_btn", new_html)
        self.assertIn("generic_forge_run_btn", edit_html)

    def test_indicatorforge_cap_settings_include_cash_position_target(self):
        root = Path(__file__).resolve().parents[1]
        new_html = (root / "app" / "templates" / "algo_new.html").read_text(encoding="utf-8")
        edit_html = (root / "app" / "templates" / "algo_edit.html").read_text(encoding="utf-8")
        self.assertIn("if_exec_cash_percent", new_html)
        self.assertIn("if_exec_cash_percent", edit_html)
        self.assertIn("if_exec_cash_source", new_html)
        self.assertIn("if_exec_cash_source", edit_html)

        defs = main._base_algo_form_defs()
        indicatorforge_keys = {p.get("key") for p in defs["scripts/indicatorforge.robinhood.py"]["params"]}
        self.assertIn("portfolio_cash_source", indicatorforge_keys)
        cash_source_def = next(
            p for p in defs["scripts/indicatorforge.robinhood.py"]["params"] if p.get("key") == "portfolio_cash_source"
        )
        self.assertEqual(cash_source_def.get("default"), "buying_power")
        self.assertIn("buying_power", {o.get("value") for o in cash_source_def.get("options", [])})
        self.assertIn("cash", {o.get("value") for o in cash_source_def.get("options", [])})

        for script_key in (
            "scripts/indicatorforge.robinhood.py",
            "scripts/entangledtickers.robinhood.py",
            "scripts/indicatorforge.schwab.py",
            "scripts/entangledtickers.schwab.py",
            "scripts/indicatorforge.crypto.robinhood.py",
        ):
            keys = {p.get("key") for p in defs[script_key]["params"]}
            self.assertIn("portfolio_cash_percent", keys)

    def test_robinhood_indicatorforge_exposes_and_preserves_overnight_toggle(self):
        root = Path(__file__).resolve().parents[1]
        new_html = (root / "app" / "templates" / "algo_new.html").read_text(encoding="utf-8")
        edit_html = (root / "app" / "templates" / "algo_edit.html").read_text(encoding="utf-8")
        self.assertIn("const showOvernightInput = Boolean(allowOvernightInput);", new_html)
        self.assertIn("const showOvernightInput = Boolean(allowOvernightInput);", edit_html)
        self.assertIn("Allow Robinhood Overnight Trading", new_html)
        self.assertIn("Allow Robinhood Overnight Trading", edit_html)

        defs = main._base_algo_form_defs()
        rh_keys = {p.get("key") for p in defs["scripts/indicatorforge.robinhood.py"]["params"]}
        self.assertIn("allow_seamless_overnight_orders", rh_keys)

        saved = main._sanitize_algorithm_params_for_script(
            '{"allow_seamless_overnight_orders": true, "allow_extended_hours_orders": true}',
            "scripts/indicatorforge.robinhood.py",
        )
        self.assertTrue(main._safe_json(saved, default={}).get("allow_seamless_overnight_orders"))

        entangled_saved = main._sanitize_algorithm_params_for_script(
            '{"allow_seamless_overnight_orders": true}',
            "scripts/entangledtickers.robinhood.py",
        )
        self.assertNotIn("allow_seamless_overnight_orders", main._safe_json(entangled_saved, default={}))


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest import mock

import app.main as main
from strategy_forge.combo_search import build_combo_candidate


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
    def test_strategy_forge_quick_partial_starts_live_panel(self):
        with tempfile.TemporaryDirectory() as tmp:
            with (
                mock.patch.object(main, "_ensure_robinhood_markets_session", return_value=(True, "")),
                mock.patch.object(main, "_strategy_forge_quick_worker", return_value=None),
            ):
                html = main._render_strategy_forge_quick_html(
                    timeframe="1h",
                    symbols="TQQQ",
                    trials=2,
                    min_trades=1,
                    min_rules=3,
                    max_rules=7,
                    broker_hint="robinhood",
                    include_extended_hours_data=False,
                    db_path=Path(tmp) / "forge.sqlite3",
                )
        self.assertIn("Strategy Forge", html)
        self.assertIn("data-forge-status='queued'", html)
        self.assertIn("/partials/strategy_forge_quick_status", html)
        self.assertIn("Rule TF pool", html)
        self.assertIn("Indicators/combo: 3-7", html)

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
        self.assertIn("New Random Set", new_html)
        self.assertIn("New Random Set", edit_html)
        self.assertIn("if_forge_seed_mode", new_html)
        self.assertIn("if_forge_seed_mode", edit_html)
        self.assertIn("generic_forge_seed_job_id", new_html)
        self.assertIn("generic_forge_seed_job_id", edit_html)
        self.assertIn("if_forge_min_rules", new_html)
        self.assertIn("if_forge_max_rules", new_html)
        self.assertIn("if_forge_min_rules", edit_html)
        self.assertIn("if_forge_max_rules", edit_html)
        self.assertIn("generic_forge_min_rules", new_html)
        self.assertIn("generic_forge_max_rules", new_html)
        self.assertIn("generic_forge_min_rules", edit_html)
        self.assertIn("generic_forge_max_rules", edit_html)

    def test_finalist_rows_show_exact_settings_and_save_button(self):
        candidate = build_combo_candidate(
            symbols=["TQQQ"],
            timeframe="5m",
            rules=[
                {"kind": "ma_cross", "timeframe": "5m", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
                {"kind": "rsi_momentum", "timeframe": "1h", "params": {"length": 14, "entry_min": 55, "exit_below": 40}},
            ],
            entry_threshold=2,
            exit_threshold=1,
        )
        public_row = main._strategy_forge_quick_public_row(
            {
                "run_id": 12,
                "candidate": candidate,
                "score": 0.42,
                "metrics": {
                    "total_return": 0.1,
                    "one_share_net_profit": 12.34,
                    "trade_count": 4,
                    "symbol_returns": {"TQQQ": 0.10},
                    "symbol_one_share_net_profit": {"TQQQ": 12.34},
                },
                "timeframes": ["5m", "1h"],
            },
            lambda _candidate: "2/2 finalist combo",
        )

        html = main._render_strategy_forge_quick_rows(
            [public_row],
            final=True,
            broker_hint="robinhood",
            include_extended_hours_data=True,
            db_path="/tmp/forge.sqlite3",
        )

        self.assertIn("Exact settings", html)
        self.assertIn("1-Share P/L", html)
        self.assertIn("$12.34", html)
        self.assertIn("Symbol returns", html)
        self.assertIn("Symbol 1-share P/L", html)
        self.assertIn("TQQQ 10.00%", html)
        self.assertIn("entry=2/2", html)
        self.assertIn("fast=5", html)
        self.assertIn("Save as Cryptid", html)
        self.assertIn("/partials/strategy_forge_save_finalist", html)

    def test_completed_strategy_forge_panel_can_continue_from_seeded_leaderboard(self):
        candidate = build_combo_candidate(
            symbols=["TQQQ"],
            timeframe="5m",
            rules=[
                {"kind": "ma_cross", "timeframe": "5m", "params": {"ma_type": "ema", "fast": 5, "slow": 20}},
                {"kind": "rsi_momentum", "timeframe": "1h", "params": {"length": 14, "entry_min": 55, "exit_below": 40}},
            ],
            entry_threshold=2,
            exit_threshold=1,
        )
        public_row = main._strategy_forge_quick_public_row(
            {
                "run_id": 99,
                "candidate": candidate,
                "score": 0.42,
                "metrics": {"total_return": 0.1, "trade_count": 4},
                "timeframes": ["5m", "1h"],
            },
            lambda _candidate: "2/2 finalist combo",
        )
        job_id = "seededtestjob"
        with main.STRATEGY_FORGE_QUICK_LOCK:
            main.STRATEGY_FORGE_QUICK_JOBS[job_id] = {
                "id": job_id,
                "status": "completed",
                "phase": "complete",
                "symbol_list": ["TQQQ"],
                "active_symbols": ["TQQQ"],
                "timeframe": "5m",
                "trial_count": 10,
                "evaluated_count": 10,
                "generation_index": 2,
                "population_index": 0,
                "population_size": 4,
                "stale_generations": 0,
                "patience": 3,
                "min_rules": 2,
                "max_rules": 5,
                "leaderboard": [public_row],
                "seed_candidates": [candidate.to_dict()],
                "seed_count": 1,
            }
        try:
            html = main._render_strategy_forge_quick_status_html(job_id)
        finally:
            with main.STRATEGY_FORGE_QUICK_LOCK:
                main.STRATEGY_FORGE_QUICK_JOBS.pop(job_id, None)

        self.assertIn("Run Again From Top Combos", html)
        self.assertIn("data-strategy-forge-action='continue'", html)
        self.assertIn("data-strategy-forge-job-id='seededtestjob'", html)
        self.assertIn("Continuation seed candidates: 1", html)

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

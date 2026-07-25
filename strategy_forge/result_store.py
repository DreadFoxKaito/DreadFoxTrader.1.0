from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import DEFAULT_DB_PATH
from .backtest_runner import BacktestResult
from .strategy_templates import StrategyCandidate


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def connect(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS strategy_runs (
          run_id INTEGER PRIMARY KEY AUTOINCREMENT,
          strategy_template TEXT NOT NULL,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          start_date TEXT,
          end_date TEXT,
          parameters_json TEXT NOT NULL,
          candidate_json TEXT NOT NULL DEFAULT '{}',
          total_return REAL NOT NULL DEFAULT 0,
          cagr REAL NOT NULL DEFAULT 0,
          sharpe REAL NOT NULL DEFAULT 0,
          sortino REAL NOT NULL DEFAULT 0,
          max_drawdown REAL NOT NULL DEFAULT 0,
          win_rate REAL NOT NULL DEFAULT 0,
          profit_factor REAL NOT NULL DEFAULT 0,
          expectancy REAL NOT NULL DEFAULT 0,
          trade_count INTEGER NOT NULL DEFAULT 0,
          avg_trade_return REAL NOT NULL DEFAULT 0,
          fees_paid REAL NOT NULL DEFAULT 0,
          slippage_estimate REAL NOT NULL DEFAULT 0,
          in_sample_score REAL NOT NULL DEFAULT 0,
          validation_score REAL NOT NULL DEFAULT 0,
          out_of_sample_score REAL NOT NULL DEFAULT 0,
          walk_forward_score REAL NOT NULL DEFAULT 0,
          robustness_score REAL NOT NULL DEFAULT 0,
          final_grade TEXT NOT NULL DEFAULT 'C',
          metrics_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trades (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          entry_time TEXT,
          exit_time TEXT,
          side TEXT,
          entry_price REAL,
          exit_price REAL,
          quantity REAL,
          gross_pnl REAL,
          fees REAL,
          slippage REAL,
          net_pnl REAL,
          exit_reason TEXT,
          bars_held INTEGER,
          trade_json TEXT NOT NULL DEFAULT '{}',
          FOREIGN KEY(run_id) REFERENCES strategy_runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS robustness_tests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER NOT NULL,
          parameter_stability_score REAL NOT NULL DEFAULT 0,
          symbol_stability_score REAL NOT NULL DEFAULT 0,
          time_window_stability_score REAL NOT NULL DEFAULT 0,
          regime_score REAL NOT NULL DEFAULT 0,
          monte_carlo_score REAL NOT NULL DEFAULT 0,
          final_grade TEXT NOT NULL DEFAULT 'C',
          details_json TEXT NOT NULL DEFAULT '{}',
          FOREIGN KEY(run_id) REFERENCES strategy_runs(run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_strategy_runs_grade ON strategy_runs(final_grade);
        CREATE INDEX IF NOT EXISTS idx_strategy_runs_template_symbol ON strategy_runs(strategy_template, symbol, timeframe);
        CREATE INDEX IF NOT EXISTS idx_trades_run_id ON trades(run_id);
        """
    )
    conn.commit()


def store_backtest_result(
    result: BacktestResult,
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    in_sample_score: float = 0.0,
    validation_score: float = 0.0,
    out_of_sample_score: float = 0.0,
    walk_forward_score: float = 0.0,
    robustness_score: float = 0.0,
    final_grade: str = "C",
) -> int:
    conn = connect(db_path)
    cur = conn.cursor()
    c = result.candidate
    m = result.metrics
    cur.execute(
        """
        INSERT INTO strategy_runs
        (strategy_template, symbol, timeframe, start_date, end_date, parameters_json, candidate_json,
         total_return, cagr, sharpe, sortino, max_drawdown, win_rate, profit_factor, expectancy,
         trade_count, avg_trade_return, fees_paid, slippage_estimate, in_sample_score, validation_score,
         out_of_sample_score, walk_forward_score, robustness_score, final_grade, metrics_json, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            c.strategy_name,
            result.symbol,
            result.timeframe,
            str(m.get("start_date") or ""),
            str(m.get("end_date") or ""),
            json.dumps(c.parameters, sort_keys=True),
            json.dumps(c.to_dict(), sort_keys=True),
            float(m.get("total_return") or 0.0),
            float(m.get("cagr") or 0.0),
            float(m.get("sharpe") or 0.0),
            float(m.get("sortino") or 0.0),
            float(m.get("max_drawdown") or 0.0),
            float(m.get("win_rate") or 0.0),
            float(m.get("profit_factor") or 0.0),
            float(m.get("expectancy") or 0.0),
            int(m.get("trade_count") or 0),
            float(m.get("avg_trade_return") or 0.0),
            float(m.get("fees_paid") or 0.0),
            float(m.get("slippage_estimate") or 0.0),
            float(in_sample_score),
            float(validation_score),
            float(out_of_sample_score),
            float(walk_forward_score),
            float(robustness_score),
            str(final_grade),
            json.dumps(m, sort_keys=True),
            utc_now(),
        ),
    )
    run_id = int(cur.lastrowid)
    for trade in result.trades:
        cur.execute(
            """
            INSERT INTO trades
            (run_id, symbol, entry_time, exit_time, side, entry_price, exit_price, quantity,
             gross_pnl, fees, slippage, net_pnl, exit_reason, bars_held, trade_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id,
                str(trade.get("symbol") or result.symbol),
                str(trade.get("entry_time") or ""),
                str(trade.get("exit_time") or ""),
                str(trade.get("side") or "long"),
                float(trade.get("entry_price") or 0.0),
                float(trade.get("exit_price") or 0.0),
                float(trade.get("quantity") or 0.0),
                float(trade.get("gross_pnl") or 0.0),
                float(trade.get("fees") or 0.0),
                float(trade.get("slippage") or 0.0),
                float(trade.get("net_pnl") or 0.0),
                str(trade.get("exit_reason") or ""),
                int(trade.get("bars_held") or 0),
                json.dumps(trade, sort_keys=True),
            ),
        )
    conn.commit()
    conn.close()
    return run_id


def store_robustness_test(
    run_id: int,
    details: dict[str, Any],
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
) -> None:
    conn = connect(db_path)
    conn.execute(
        """
        INSERT INTO robustness_tests
        (run_id, parameter_stability_score, symbol_stability_score, time_window_stability_score,
         regime_score, monte_carlo_score, final_grade, details_json)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            int(run_id),
            float(details.get("parameter_stability_score") or 0.0),
            float(details.get("symbol_stability_score") or 0.0),
            float(details.get("time_window_stability_score") or 0.0),
            float(details.get("regime_score") or 0.0),
            float(details.get("monte_carlo_score") or 0.0),
            str(details.get("final_grade") or "C"),
            json.dumps(details, sort_keys=True),
        ),
    )
    conn.commit()
    conn.close()


def get_run(run_id: int, *, db_path: str | Path = DEFAULT_DB_PATH) -> Optional[dict[str, Any]]:
    conn = connect(db_path)
    row = conn.execute("SELECT * FROM strategy_runs WHERE run_id=?", (int(run_id),)).fetchone()
    conn.close()
    if row is None:
        return None
    out = dict(row)
    out["parameters"] = json.loads(out.get("parameters_json") or "{}")
    out["candidate"] = json.loads(out.get("candidate_json") or "{}")
    out["metrics"] = json.loads(out.get("metrics_json") or "{}")
    return out


def get_trades(run_id: int, *, db_path: str | Path = DEFAULT_DB_PATH) -> list[dict[str, Any]]:
    conn = connect(db_path)
    rows = conn.execute("SELECT * FROM trades WHERE run_id=? ORDER BY id", (int(run_id),)).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def list_runs(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    sort: str = "walk_forward_score",
    top: int = 25,
) -> list[dict[str, Any]]:
    allowed = {
        "total_return",
        "sharpe",
        "sortino",
        "max_drawdown",
        "profit_factor",
        "expectancy",
        "out_of_sample_score",
        "walk_forward_score",
        "robustness_score",
        "trade_count",
        "created_at",
    }
    sort_key = sort if sort in allowed else "walk_forward_score"
    direction = "ASC" if sort_key == "max_drawdown" else "DESC"
    conn = connect(db_path)
    rows = conn.execute(
        f"SELECT * FROM strategy_runs ORDER BY {sort_key} {direction}, run_id DESC LIMIT ?",
        (max(1, int(top)),),
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def update_run_grade(run_id: int, grade: str, *, db_path: str | Path = DEFAULT_DB_PATH) -> None:
    conn = connect(db_path)
    conn.execute("UPDATE strategy_runs SET final_grade=? WHERE run_id=?", (str(grade), int(run_id)))
    conn.commit()
    conn.close()


def candidate_from_run(row: dict[str, Any]) -> StrategyCandidate:
    payload = row.get("candidate")
    if not isinstance(payload, dict):
        payload = json.loads(row.get("candidate_json") or "{}")
    return StrategyCandidate.from_dict(payload)

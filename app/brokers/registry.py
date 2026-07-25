from __future__ import annotations

import html
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from . import (
    BrokerAuthError,
    BrokerConnector,
    BrokerConnectorError,
    NormalizedAccount,
    NormalizedPortfolioSnapshot,
    NormalizedPosition,
    safe_float,
)

# Optional imports — Step 8/9 will provide these connectors.
# We keep fallbacks so the app continues to run while you build file-by-file.
try:
    from .schwab_connector import SchwabConnector  # type: ignore
except Exception:
    SchwabConnector = None  # type: ignore

try:
    from .robinhood_connector import RobinhoodConnector  # type: ignore
except Exception:
    RobinhoodConnector = None  # type: ignore

# Fallback: use existing schwab_client.py directly if SchwabConnector is not present yet.
try:
    from ..schwab_client import SchwabAuthError, fetch_portfolio_snapshot
except Exception:  # pragma: no cover
    SchwabAuthError = None  # type: ignore
    fetch_portfolio_snapshot = None  # type: ignore

_SNAPSHOT_CACHE_TTL = 60
_SNAPSHOT_CACHE: dict[tuple[str, int], dict[str, Any]] = {}

PORTFOLIO_LOG_INTERVAL_SEC = int(os.getenv("CRYPTID_PORTFOLIO_LOG_INTERVAL_SEC", "900"))
PORTFOLIO_LOG_RETENTION_DAYS = int(os.getenv("CRYPTID_PORTFOLIO_LOG_RETENTION_DAYS", "400"))
PORTFOLIO_CHART_LOOKBACK_DAYS = int(os.getenv("CRYPTID_PORTFOLIO_CHART_LOOKBACK_DAYS", "365"))
PORTFOLIO_CHART_MAX_POINTS = int(os.getenv("CRYPTID_PORTFOLIO_CHART_MAX_POINTS", "96"))


# -------------------------
# SQLite helpers
# -------------------------
def _db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _utc_ts() -> int:
    return int(time.time())


def _safe_json(s: str, default: Any) -> Any:
    try:
        return json.loads(s) if s else default
    except Exception:
        return default


def _get_connection(conn: sqlite3.Connection, connection_id: int) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM broker_connections WHERE id=?", (int(connection_id),))
    return cur.fetchone()


def _list_connections(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM broker_connections ORDER BY id DESC")
    return list(cur.fetchall())


def _update_connection_status(db_path: str, connection_id: int, status: str, metadata: dict[str, Any] | None = None) -> None:
    conn = _db(db_path)
    cur = conn.cursor()
    now = _utc_ts()
    if metadata is None:
        cur.execute("UPDATE broker_connections SET status=?, updated_ts=? WHERE id=?", (status, now, int(connection_id)))
    else:
        cur.execute(
            "UPDATE broker_connections SET status=?, metadata_json=?, updated_ts=? WHERE id=?",
            (status, json.dumps(metadata), now, int(connection_id)),
        )
    conn.commit()
    conn.close()


# -------------------------
# Formatting helpers
# -------------------------
def fmt_money(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        return f"${v:,.2f}"
    except Exception:
        return "—"


def fmt_num(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        if abs(v - int(v)) < 1e-9:
            return f"{int(v)}"
        return f"{v:,.4f}"
    except Exception:
        return "—"


def fmt_money_short(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        av = abs(v)
        if av >= 1_000_000_000:
            return f"${v/1_000_000_000:.1f}b"
        if av >= 1_000_000:
            return f"${v/1_000_000:.1f}m"
        if av >= 1_000:
            return f"${v/1_000:.1f}k"
        return f"${v:,.2f}"
    except Exception:
        return "—"


def fmt_percent(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x) * 100.0
        return f"{v:.2f}%"
    except Exception:
        return "—"


def _pick_balance(bal: dict[str, Any], keys: list[str]) -> Any:
    for k in keys:
        if k in bal and bal.get(k) is not None:
            return bal.get(k)
    return None


def _position_rank(p: NormalizedPosition) -> float:
    mv = safe_float(getattr(p, "market_value", None))
    if mv is not None:
        return mv
    qty = safe_float(getattr(p, "quantity", None))
    if qty is not None:
        return qty
    return 0.0


def _raw_first_float(raw: dict[str, Any], keys: list[str]) -> Optional[float]:
    for key in keys:
        val = safe_float(raw.get(key))
        if val is not None:
            return val
    return None


def _position_price_multiplier(p: NormalizedPosition) -> float:
    raw = getattr(p, "raw", None)
    if not isinstance(raw, dict):
        return 1.0
    blobs: list[dict[str, Any]] = []
    for key in ("position", "market_data", "instrument", "quote"):
        val = raw.get(key)
        if isinstance(val, dict):
            blobs.append(val)
    blobs.append(raw)
    for blob in blobs:
        mult = _raw_first_float(
            blob,
            ["shares_per_contract", "trade_value_multiplier", "contract_multiplier", "multiplier"],
        )
        if mult is not None and mult > 0:
            return float(mult)
    return 1.0


def _position_previous_close(p: NormalizedPosition) -> Optional[float]:
    raw = getattr(p, "raw", None)
    if not isinstance(raw, dict):
        return None
    blobs: list[dict[str, Any]] = []
    for key in ("quote", "market_data", "instrument", "position"):
        val = raw.get(key)
        if isinstance(val, dict):
            blobs.append(val)
    blobs.append(raw)
    for blob in blobs:
        candidates: list[dict[str, Any]] = [blob]
        nested_quote = blob.get("quote") if isinstance(blob.get("quote"), dict) else None
        nested_regular = blob.get("regular") if isinstance(blob.get("regular"), dict) else None
        if nested_quote is not None:
            candidates.append(nested_quote)
        if nested_regular is not None:
            candidates.append(nested_regular)
        for source in candidates:
            prev = _raw_first_float(
                source,
                [
                    "previous_close",
                    "adjusted_previous_close",
                    "previous_close_price",
                    "prior_close",
                    "close_price",
                    "closePrice",
                    "regularMarketLastPrice",
                    "previousClose",
                    "prevClose",
                ],
            )
            if prev is not None and prev > 0:
                return float(prev)
    return None


def _position_day_pl(p: NormalizedPosition) -> Optional[float]:
    raw = getattr(p, "raw", None)
    if isinstance(raw, dict):
        blobs: list[dict[str, Any]] = []
        for key in ("position", "market_data", "quote", "instrument"):
            val = raw.get(key)
            if isinstance(val, dict):
                blobs.append(val)
        blobs.append(raw)
        for blob in blobs:
            candidates: list[dict[str, Any]] = [blob]
            nested_quote = blob.get("quote") if isinstance(blob.get("quote"), dict) else None
            if nested_quote is not None:
                candidates.append(nested_quote)
            for source in candidates:
                day_pl = _raw_first_float(
                    source,
                    [
                        "currentDayProfitLoss",
                        "current_day_profit_loss",
                        "day_profit_loss",
                        "today_profit_loss",
                        "todays_profit_loss",
                        "mark_to_market_profit_loss",
                    ],
                )
                if day_pl is not None:
                    return float(day_pl)

    prev_close = _position_previous_close(p)
    market_price = safe_float(getattr(p, "market_price", None))
    quantity = safe_float(getattr(p, "quantity", None))
    if prev_close is None or market_price is None or quantity is None:
        return None
    return (float(market_price) - float(prev_close)) * float(quantity) * _position_price_multiplier(p)


def _pl_class(pl: Optional[float]) -> str:
    if pl is None:
        return ""
    if pl > 0:
        return "ticker-profit"
    if pl < 0:
        return "ticker-loss"
    return ""


def _fmt_money_signed(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        if v > 0:
            return f"+${abs(v):,.2f}"
        if v < 0:
            return f"-${abs(v):,.2f}"
        return "$0.00"
    except Exception:
        return "—"


def _fmt_percent_signed(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x) * 100.0
        if v > 0:
            return f"+{abs(v):.2f}%"
        if v < 0:
            return f"-{abs(v):.2f}%"
        return "0.00%"
    except Exception:
        return "—"


def _total_equity_from_snapshots(snaps: list[NormalizedPortfolioSnapshot]) -> Optional[float]:
    total = 0.0
    has_val = False
    for snap in snaps:
        for a in snap.accounts:
            bal = a.balances or {}
            if not isinstance(bal, dict):
                bal = {}
            liq = _pick_balance(
                bal,
                ["liquidationValue", "accountValue", "extended_hours_equity", "equity", "netLiquidation", "totalAccountValue"],
            )
            liq_val = safe_float(liq)
            if liq_val is not None:
                total += liq_val
                has_val = True
    return total if has_val else None


def _record_portfolio_equity(db_path: str, total_equity: Optional[float]) -> None:
    if total_equity is None:
        return
    if PORTFOLIO_LOG_INTERVAL_SEC <= 0:
        return
    conn = _db(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT ts FROM portfolio_equity_log ORDER BY ts DESC LIMIT 1")
        row = cur.fetchone()
        now = _utc_ts()
        if row and (now - int(row["ts"])) < PORTFOLIO_LOG_INTERVAL_SEC:
            return
        cur.execute(
            "INSERT INTO portfolio_equity_log (ts, total_equity) VALUES (?, ?)",
            (now, float(total_equity)),
        )
        if PORTFOLIO_LOG_RETENTION_DAYS > 0:
            cutoff = now - int(PORTFOLIO_LOG_RETENTION_DAYS) * 86400
            cur.execute("DELETE FROM portfolio_equity_log WHERE ts < ?", (cutoff,))
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def _start_of_day_ts(ts: int) -> int:
    lt = time.localtime(ts)
    return int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst)))


def _select_equity_before(conn: sqlite3.Connection, ts: int) -> Optional[tuple[int, float]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT ts, total_equity FROM portfolio_equity_log WHERE ts <= ? ORDER BY ts DESC LIMIT 1",
        (int(ts),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return (int(row["ts"]), float(row["total_equity"]))


def _select_equity_after(conn: sqlite3.Connection, ts: int) -> Optional[tuple[int, float]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT ts, total_equity FROM portfolio_equity_log WHERE ts >= ? ORDER BY ts ASC LIMIT 1",
        (int(ts),),
    )
    row = cur.fetchone()
    if not row:
        return None
    return (int(row["ts"]), float(row["total_equity"]))


def _perf_entry(current_equity: float, baseline: Optional[tuple[int, float]]) -> Optional[dict[str, Any]]:
    if baseline is None:
        return None
    baseline_ts, baseline_equity = baseline
    delta = current_equity - float(baseline_equity)
    pct = None
    if baseline_equity:
        pct = delta / float(baseline_equity)
    return {
        "baseline_ts": baseline_ts,
        "baseline_equity": float(baseline_equity),
        "delta": float(delta),
        "delta_pct": pct,
    }


def _portfolio_performance_summary(db_path: str, current_equity: Optional[float]) -> dict[str, Any]:
    conn = _db(db_path)
    latest_logged_equity: Optional[float] = None
    latest_logged_ts: Optional[int] = None
    try:
        cur = conn.cursor()
        cur.execute("SELECT ts, total_equity FROM portfolio_equity_log ORDER BY ts DESC LIMIT 1")
        row = cur.fetchone()
        if row is not None:
            v = safe_float(row["total_equity"])
            if v is not None:
                latest_logged_equity = float(v)
                latest_logged_ts = int(row["ts"])
    except Exception:
        pass

    effective_equity = float(current_equity) if current_equity is not None else latest_logged_equity
    summary: dict[str, Any] = {
        "as_of": latest_logged_ts if latest_logged_ts is not None else _utc_ts(),
        "current_equity": effective_equity,
        "day": None,
        "month": None,
        "year": None,
    }
    if effective_equity is None:
        conn.close()
        return summary

    try:
        now = _utc_ts()
        day_start = _start_of_day_ts(now)
        day_base = _select_equity_after(conn, day_start)
        if day_base is None:
            day_base = _select_equity_before(conn, day_start)

        month_target = now - 30 * 86400
        month_base = _select_equity_before(conn, month_target)
        if month_base is None:
            month_base = _select_equity_after(conn, month_target)

        year_target = now - 365 * 86400
        year_base = _select_equity_before(conn, year_target)
        if year_base is None:
            year_base = _select_equity_after(conn, year_target)

        summary["day"] = _perf_entry(float(effective_equity), day_base)
        summary["month"] = _perf_entry(float(effective_equity), month_base)
        summary["year"] = _perf_entry(float(effective_equity), year_base)
    except Exception:
        pass
    finally:
        conn.close()

    return summary


def _portfolio_equity_series(db_path: str, *, lookback_days: int, max_points: int) -> list[float]:
    conn = _db(db_path)
    try:
        now = _utc_ts()
        cutoff = now - max(1, int(lookback_days)) * 86400
        cur = conn.cursor()
        cur.execute(
            "SELECT total_equity FROM portfolio_equity_log WHERE ts >= ? ORDER BY ts ASC",
            (int(cutoff),),
        )
        vals = [safe_float(r["total_equity"]) for r in cur.fetchall()]
        pts = [float(v) for v in vals if v is not None]
        if len(pts) <= max(2, int(max_points)):
            return pts

        n = len(pts)
        m = max(2, int(max_points))
        out: list[float] = []
        for i in range(m):
            idx = int(round(i * (n - 1) / (m - 1)))
            out.append(float(pts[idx]))
        return out
    except Exception:
        return []
    finally:
        conn.close()


def _render_performance_sparkline(equity_points: list[float]) -> str:
    if len(equity_points) < 2:
        return ""

    w = 190.0
    h = 42.0
    pad = 3.0
    lo = min(equity_points)
    hi = max(equity_points)
    rng = (hi - lo) if hi != lo else 1.0
    n = len(equity_points)

    coords: list[str] = []
    for i, val in enumerate(equity_points):
        x = pad + (i * (w - 2 * pad) / (n - 1))
        y = h - pad - ((float(val) - lo) / rng) * (h - 2 * pad)
        coords.append(f"{x:.2f},{y:.2f}")

    trend_up = equity_points[-1] >= equity_points[0]
    trend_cls = "up" if trend_up else "down"
    stroke = "#c8ff4d" if trend_up else "#fb7185"
    bg_line = "rgba(255,255,255,0.12)"
    x_end = pad + ((n - 1) * (w - 2 * pad) / (n - 1))
    y_end = h - pad - ((float(equity_points[-1]) - lo) / rng) * (h - 2 * pad)
    return (
        f"<div class='perf-sparkline {trend_cls}' title='Portfolio value trend'>"
        f"<svg viewBox='0 0 {int(w)} {int(h)}' preserveAspectRatio='none' aria-label='Portfolio trend'>"
        f"<line x1='{pad:.2f}' y1='{h - pad:.2f}' x2='{w - pad:.2f}' y2='{h - pad:.2f}' "
        f"stroke='{bg_line}' stroke-width='1' />"
        f"<polyline points='{' '.join(coords)}' class='line' stroke='{stroke}' />"
        f"<circle cx='{x_end:.2f}' cy='{y_end:.2f}' r='2.2' fill='{stroke}' />"
        "</svg>"
        "</div>"
    )


def _render_metric_sparkline(equity_points: list[float]) -> str:
    pts = [float(v) for v in equity_points]
    if len(pts) == 1:
        pts = [pts[0], pts[0]]
    if len(pts) < 2:
        return ""
    w = 118.0
    h = 34.0
    pad = 2.5
    lo = min(pts)
    hi = max(pts)
    rng = (hi - lo) if hi != lo else 1.0
    n = len(pts)
    coords: list[str] = []
    for i, val in enumerate(pts):
        x = pad + (i * (w - 2 * pad) / (n - 1))
        y = h - pad - ((float(val) - lo) / rng) * (h - 2 * pad)
        coords.append(f"{x:.2f},{y:.2f}")
    trend_up = pts[-1] >= pts[0]
    stroke = "#c8ff4d" if trend_up else "#fb7185"

    # Bars show each point's relative gain/loss versus the last point in this timeframe window.
    # green: current(last) is above that point (gain), red: current(last) is below that point (loss).
    last = pts[-1]
    deltas = [float(p) - float(last) for p in pts]
    max_abs = max(abs(d) for d in deltas) if deltas else 0.0
    if max_abs <= 0:
        max_abs = 1.0
    bar_html: list[str] = []
    for d in deltas:
        rel = abs(float(d)) / max_abs
        h_bar = 4.0 + rel * 14.0
        if d > 0:
            c = "#fb7185"
        elif d < 0:
            c = "#c8ff4d"
        else:
            c = "rgba(255,255,255,0.35)"
        bar_html.append(
            f"<span style='display:block;width:2.4px;height:{h_bar:.2f}px;"
            f"background:{c};border-radius:1px;'></span>"
        )

    return (
        "<div style='display:flex;flex-direction:column;align-items:flex-end;gap:1px;'>"
        "<svg viewBox='0 0 118 34' preserveAspectRatio='none' "
        "style='display:block;width:118px;height:34px;opacity:0.95;'>"
        f"<polyline points='{' '.join(coords)}' fill='none' stroke='{stroke}' "
        "stroke-width='2.3' stroke-linecap='round' stroke-linejoin='round'/>"
        "</svg>"
        "<div style='display:flex;align-items:flex-end;justify-content:flex-end;gap:0.6px;"
        "width:118px;height:18px;'>"
        + "".join(bar_html)
        + "</div>"
        "</div>"
    )


def _render_performance_badges(perf: dict[str, Any], *, db_path: Optional[str] = None) -> str:
    if not perf or perf.get("current_equity") is None:
        return ""
    metric_charts: dict[str, str] = {}
    if db_path:
        windows = {
            "day": (1, 36),
            "month": (30, 60),
            "year": (365, 96),
        }
        for key, (days, pts) in windows.items():
            series = _portfolio_equity_series(db_path, lookback_days=days, max_points=pts)
            metric_charts[key] = _render_metric_sparkline(series)
    parts: list[str] = []
    for label, key in (("Day", "day"), ("1M", "month"), ("1Y", "year")):
        entry = perf.get(key) or {}
        delta = entry.get("delta")
        pct = entry.get("delta_pct")
        if delta is None:
            text = "—"
            cls = ""
        else:
            text = f"{_fmt_money_signed(delta)} ({_fmt_percent_signed(pct)})"
            cls = _pl_class(float(delta))
        class_attr = f"badge {cls}".strip()
        chart = metric_charts.get(key, "")
        if chart:
            parts.append(
                f"<span class='{class_attr}' style='display:inline-flex;align-items:center;gap:8px;'>"
                f"<span>{label} {html.escape(text)}</span>{chart}</span>"
            )
        else:
            parts.append(f"<span class='{class_attr}'>{label} {html.escape(text)}</span>")
    return "<div class='row' style='margin-top:6px;'>" + "".join(parts) + "</div>"

def _render_debug_details(metadata: dict[str, Any]) -> str:
    if not metadata:
        return ""
    err = metadata.get("error")
    debug = metadata.get("debug")
    if not err and not debug:
        return ""

    parts: list[str] = []
    if err:
        parts.append(f"<div class='small'><b>Error:</b> {html.escape(str(err))}</div>")
    if debug:
        try:
            dbg_txt = json.dumps(debug, indent=2, sort_keys=True)
        except Exception:
            dbg_txt = str(debug)
        parts.append(
            "<pre class='small' style='white-space:pre-wrap; margin-top:6px;'>"
            f"{html.escape(dbg_txt)}"
            "</pre>"
        )
    return "<details style='margin-top:8px'><summary class='small'>Debug</summary>" + "".join(parts) + "</details>"


def get_portfolio_bubbles_html(*, db_path: str, pl_mode: str = "dollar") -> str:
    """
    Dashboard bubbles: per-connection Net Liq + Cash + condensed holdings.
    """
    pl_mode = (pl_mode or "dollar").strip().lower()
    if pl_mode not in ("dollar", "percent", "pct", "%"):
        pl_mode = "dollar"
    if pl_mode in ("percent", "pct", "%"):
        pl_mode = "percent"
    else:
        pl_mode = "dollar"
    conn = _db(db_path)
    rows = _list_connections(conn)
    conn.close()

    if not rows:
        return "<div class='small'>No broker connections yet. Add one on the Broker page.</div>"

    snaps = _fetch_snapshots(db_path)
    snap_map = {s.connection_id: s for s in snaps}

    blocks: list[str] = []
    total_equity = _total_equity_from_snapshots(snaps)
    perf = _portfolio_performance_summary(db_path, total_equity)
    perf_html = _render_performance_badges(perf, db_path=db_path)
    if perf_html:
        blocks.append("<div style='margin-bottom:10px;'>" + perf_html + "</div>")
    blocks.append("<div class='portfolio-bubble-grid'>")

    for r in rows:
        cid = int(r["id"])
        label = html.escape(str(r["label"]))
        broker_id = html.escape(str(r["broker"]))
        status = str(r["status"])
        badge = "ok" if status == "connected" else ("warn" if status in ("needs_auth", "needs_attention") else "bad")

        snap = snap_map.get(cid)
        total_liq = 0.0
        total_cash = 0.0
        total_available_funds = 0.0
        total_margin_buying_power = 0.0
        has_liq = False
        has_cash = False
        has_available_funds = False
        has_margin_buying_power = False
        positions: list[NormalizedPosition] = []

        if snap:
            for a in snap.accounts:
                bal = a.balances or {}
                if not isinstance(bal, dict):
                    bal = {}
                liq = _pick_balance(
                    bal,
                    ["liquidationValue", "accountValue", "extended_hours_equity", "equity", "netLiquidation", "totalAccountValue"],
                )
                cash = _pick_balance(
                    bal,
                    ["cashBalance", "cashAvailableForTrading", "availableFundsForTrading", "cash"],
                )
                available_funds = _pick_balance(
                    bal,
                    ["availableFunds"],
                )
                margin_buying_power = _pick_balance(
                    bal,
                    ["buyingPower"],
                )
                if liq is not None:
                    try:
                        total_liq += float(liq)
                        has_liq = True
                    except Exception:
                        pass
                if cash is not None:
                    try:
                        total_cash += float(cash)
                        has_cash = True
                    except Exception:
                        pass
                if available_funds is not None:
                    try:
                        total_available_funds += float(available_funds)
                        has_available_funds = True
                    except Exception:
                        pass
                if margin_buying_power is not None:
                    try:
                        total_margin_buying_power += float(margin_buying_power)
                        has_margin_buying_power = True
                    except Exception:
                        pass
                positions.extend(a.positions or [])

        broker_lower = str(r["broker"]).lower()
        opt_count = 0
        opt_mv = 0.0
        opt_pl = 0.0
        opt_mv_found = False
        opt_pl_found = False
        if positions:
            for p in positions:
                asset = str(getattr(p, "asset_type", "") or "").lower()
                if "option" not in asset:
                    continue
                opt_count += 1
                mv = safe_float(getattr(p, "market_value", None))
                if mv is None:
                    qty = safe_float(getattr(p, "quantity", None))
                    price = safe_float(getattr(p, "market_price", None))
                    if qty is not None and price is not None:
                        mv = qty * price
                if mv is not None:
                    opt_mv += float(mv)
                    opt_mv_found = True

                pl = safe_float(getattr(p, "unrealized_pl", None))
                if pl is None:
                    avg = safe_float(getattr(p, "average_price", None))
                    qty = safe_float(getattr(p, "quantity", None))
                    price = safe_float(getattr(p, "market_price", None))
                    if avg is not None and qty is not None and price is not None:
                        pl = (price - avg) * qty
                if pl is not None:
                    opt_pl += float(pl)
                    opt_pl_found = True

        total_positions = len(positions)
        holdings_label = "No data yet" if not snap else ("No positions" if not total_positions else f"{total_positions} positions")
        holdings_rows: list[str] = []
        if total_positions:
            agg: dict[str, dict[str, Any]] = {}
            for p in positions:
                sym = str(p.symbol or "-")
                asset = str(p.asset_type or "").strip()
                asset_label = asset or "-"
                key = f"{sym}:{asset_label}" if asset_label not in ("", "-") else sym
                entry = agg.setdefault(
                    key,
                    {
                        "symbol": sym,
                        "asset": asset_label,
                        "qty": 0.0,
                        "mv": 0.0,
                        "pl": 0.0,
                        "cost": 0.0,
                        "has_qty": False,
                        "has_mv": False,
                        "has_pl": False,
                        "has_cost": False,
                    },
                )
                qty = safe_float(p.quantity)
                if qty is not None:
                    entry["qty"] += qty
                    entry["has_qty"] = True
                mv = safe_float(p.market_value)
                if mv is not None:
                    entry["mv"] += mv
                    entry["has_mv"] = True
                pl = safe_float(p.unrealized_pl)
                if pl is not None:
                    entry["pl"] += pl
                    entry["has_pl"] = True
                avg = safe_float(p.average_price)
                if avg is not None and qty is not None:
                    entry["cost"] += avg * qty
                    entry["has_cost"] = True

            def _rank(item: dict[str, Any]) -> float:
                if item.get("has_mv"):
                    return float(item.get("mv") or 0.0)
                if item.get("has_qty"):
                    return float(item.get("qty") or 0.0)
                return 0.0

            for item in sorted(agg.values(), key=_rank, reverse=True):
                qty_txt = fmt_num(item.get("qty")) if item.get("has_qty") else "—"
                mv_txt = fmt_money_short(item.get("mv")) if item.get("has_mv") else "—"
                pl_val = item.get("pl") if item.get("has_pl") else None
                pct_val = None
                if pl_val is not None:
                    base = None
                    cost = item.get("cost") if item.get("has_cost") else None
                    mv = item.get("mv") if item.get("has_mv") else None
                    if cost:
                        base = cost
                    elif mv:
                        base = mv
                    if base:
                        pct_val = pl_val / base
                if pl_mode == "percent":
                    pl_txt = fmt_percent(pct_val)
                else:
                    pl_txt = fmt_money_short(pl_val)

                sym_class = _pl_class(pl_val)
                sym_attr = f" class='{sym_class}'" if sym_class else ""
                pl_attr = f" class='num {sym_class}'" if sym_class else " class='num'"
                num_attr = f" class='num {sym_class}'" if sym_class else " class='num'"
                holdings_rows.append(
                    "<tr>"
                    f"<td{sym_attr}>{html.escape(str(item.get('symbol') or '-'))}</td>"
                    f"<td class='muted'>{html.escape(str(item.get('asset') or '-'))}</td>"
                    f"<td{num_attr}>{html.escape(qty_txt)}</td>"
                    f"<td{num_attr}>{html.escape(mv_txt)}</td>"
                    f"<td{pl_attr}>{html.escape(pl_txt)}</td>"
                    "</tr>"
                )

        liq_txt = fmt_money(total_liq) if snap and has_liq else "—"
        cash_txt = fmt_money(total_cash) if snap and has_cash else "—"
        available_funds_badge = ""
        if snap and has_available_funds:
            available_funds_badge = f"<span class='badge'>Available Funds {fmt_money(total_available_funds)}</span>"
        margin_buying_power_badge = ""
        if snap and has_margin_buying_power:
            margin_buying_power_badge = f"<span class='badge'>Margin BP {fmt_money(total_margin_buying_power)}</span>"
        options_row = ""
        if snap and (broker_lower == "robinhood" or opt_count > 0):
            opt_mv_val: Optional[float]
            opt_pl_val: Optional[float]
            if opt_count == 0:
                opt_mv_val = 0.0
                opt_pl_val = 0.0
            else:
                opt_mv_val = opt_mv if opt_mv_found else None
                opt_pl_val = opt_pl if opt_pl_found else None
            opt_mv_txt = fmt_money(opt_mv_val) if opt_mv_val is not None else "—"
            opt_pl_txt = _fmt_money_signed(opt_pl_val) if opt_pl_val is not None else "—"
            opt_pl_cls = _pl_class(opt_pl_val) if opt_pl_val is not None else ""
            opt_pl_attr = f"badge {opt_pl_cls}".strip()
            options_row = (
                "<div class='row' style='margin-top:6px;'>"
                f"<span class='badge'>Options {opt_mv_txt}</span>"
                f"<span class='{opt_pl_attr}'>Opt P/L {html.escape(opt_pl_txt)}</span>"
                "</div>"
            )

        holdings_table = ""
        if holdings_rows:
            pl_header = "P/L %" if pl_mode == "percent" else "P/L $"
            holdings_table = (
                "<div class='holdings-table-wrap'>"
                "<table class='holdings-table'>"
                "<thead><tr><th>Ticker</th><th>Type</th><th class='num'>Qty</th><th class='num'>Value</th>"
                f"<th class='num'>{pl_header}</th></tr></thead>"
                "<tbody>"
                + "".join(holdings_rows)
                + "</tbody></table></div>"
            )

        holdings_html = f"<div class='small' style='margin-top:8px;'>Holdings: {html.escape(holdings_label)}</div>"
        if holdings_table:
            holdings_html += holdings_table

        blocks.append(
            "<div class='card portfolio-bubble'>"
            "<div class='row' style='justify-content:space-between'>"
            f"<div><b>{label}</b><div class='small'>{broker_id} · id {cid}</div></div>"
            f"<span class='badge {badge}'>{html.escape(status)}</span>"
            "</div>"
            "<div class='row' style='margin-top:10px;'>"
            f"<span class='badge'>Net Liq {liq_txt}</span>"
            f"<span class='badge'>Cash {cash_txt}</span>"
            f"{available_funds_badge}"
            f"{margin_buying_power_badge}"
            "</div>"
            f"{options_row}"
            f"{holdings_html}"
            "<div class='row' style='margin-top:10px;'>"
            f"<a class='btn' href='/broker?connection_id={cid}'>Open</a>"
            "</div>"
            "</div>"
        )

    blocks.append("</div>")
    return "".join(blocks)


# -------------------------
# Connector Registry
# -------------------------
def _connectors() -> dict[str, BrokerConnector]:
    """
    Return instantiated connectors (when available). Always includes a Schwab fallback if possible.
    """
    out: dict[str, BrokerConnector] = {}

    if SchwabConnector is not None:
        out["schwab"] = SchwabConnector()
    else:
        # Fallback Schwab connector using schwab_client.py (pre-Step-8)
        if fetch_portfolio_snapshot is not None:

            class _FallbackSchwab(BrokerConnector):
                broker_id = "schwab"
                broker_name = "Schwab"

                def link(self, *, db_path: str, label: str, **kwargs: Any) -> int:  # pragma: no cover
                    raise BrokerConnectorError("Use /broker/connect for Schwab OAuth linking (legacy flow).")

                def unlink(self, *, db_path: str, connection_id: int) -> None:  # pragma: no cover
                    # Unlink is handled via main's /broker/disconnect for legacy token file.
                    return None

                def portfolio_snapshot(self, *, db_path: str, connection_id: int) -> NormalizedPortfolioSnapshot:
                    data_dir = Path(db_path).resolve().parent
                    token_path = data_dir / "schwab_token.json"
                    if not token_path.exists():
                        raise BrokerAuthError("Schwab token not found. Connect Schwab first.")

                    try:
                        token = json.loads(token_path.read_text())
                    except Exception:
                        raise BrokerAuthError("Schwab token file unreadable. Reconnect Schwab.")

                    def _write_tok(tok: dict[str, Any]) -> None:
                        try:
                            token_path.write_text(json.dumps(tok, indent=2))
                        except Exception:
                            pass

                    try:
                        snap = fetch_portfolio_snapshot(token=token, token_write_func=_write_tok)
                    except Exception as e:
                        # Normalize auth vs non-auth.
                        if SchwabAuthError is not None and isinstance(e, SchwabAuthError):
                            raise BrokerAuthError(str(e), status_code=getattr(e, "status_code", 401))
                        raise BrokerConnectorError(f"Schwab portfolio fetch failed: {e}")

                    accounts: list[NormalizedAccount] = []
                    for a in (snap.get("accounts") or []):
                        bal = a.get("balances") if isinstance(a, dict) else None
                        if not isinstance(bal, dict):
                            bal = {}
                        pos_in = a.get("positions") if isinstance(a, dict) else None
                        positions: list[NormalizedPosition] = []
                        if isinstance(pos_in, list):
                            for p in pos_in:
                                if not isinstance(p, dict):
                                    continue
                                inst = p.get("instrument")
                                inst = inst if isinstance(inst, dict) else {}
                                symbol = inst.get("symbol") or inst.get("cusip") or "—"
                                long_qty = safe_float(p.get("longQuantity"))
                                short_qty = safe_float(p.get("shortQuantity"))
                                if long_qty is not None and abs(long_qty) > 0:
                                    qty = float(long_qty)
                                elif short_qty is not None and abs(short_qty) > 0:
                                    qty = -float(short_qty)
                                else:
                                    qty = safe_float(p.get("quantity"))

                                avg_price = safe_float(p.get("averagePrice") or p.get("avgPrice"))
                                market_price = safe_float(p.get("marketPrice") or p.get("currentDayPrice"))
                                market_value = safe_float(p.get("marketValue"))

                                unrealized_pl = None
                                for key in (
                                    "longOpenProfitLoss",
                                    "shortOpenProfitLoss",
                                    "openProfitLoss",
                                    "open_profit_loss",
                                    "unrealizedProfitLoss",
                                    "unrealized_profit_loss",
                                ):
                                    val = safe_float(p.get(key))
                                    if val is not None:
                                        unrealized_pl = float(val)
                                        break
                                if unrealized_pl is None and market_value is not None and avg_price is not None and qty is not None:
                                    unrealized_pl = float(market_value) - (float(avg_price) * float(qty))
                                if unrealized_pl is None and market_price is not None and avg_price is not None and qty is not None:
                                    unrealized_pl = (float(market_price) - float(avg_price)) * float(qty)

                                positions.append(
                                    NormalizedPosition(
                                        symbol=str(symbol),
                                        quantity=qty,
                                        average_price=avg_price,
                                        market_price=market_price,
                                        market_value=market_value,
                                        unrealized_pl=unrealized_pl,
                                        asset_type=str(inst.get("assetType") or ""),
                                        raw=p,
                                    )
                                )
                        accounts.append(
                            NormalizedAccount(
                                account_id=str(a.get("accountNumber", "—")),
                                account_type=str(a.get("type", "")),
                                balances=bal,
                                positions=positions,
                                raw=a,
                            )
                        )

                    return NormalizedPortfolioSnapshot(
                        broker="schwab",
                        label="Schwab",
                        connection_id=int(connection_id),
                        accounts=accounts,
                        raw=snap,
                    )

            out["schwab"] = _FallbackSchwab()

    if RobinhoodConnector is not None:
        out["robinhood"] = RobinhoodConnector()

    return out


def get_all_supported_brokers() -> list[dict[str, Any]]:
    """
    For UI dropdowns/cards. Note: Robinhood support becomes fully operational in Step 9.
    """
    supported = [
        {"id": "schwab", "name": "Schwab"},
        {"id": "robinhood", "name": "Robinhood"},
    ]
    return supported


def _get_connector_or_raise(broker_id: str) -> BrokerConnector:
    conn_map = _connectors()
    c = conn_map.get(broker_id)
    if not c:
        raise BrokerConnectorError(f"Broker connector not available yet: {broker_id}")
    return c


# -------------------------
# Public API used by main.py
# -------------------------
def unlink_connection(*, db_path: str, connection_id: int) -> None:
    conn = _db(db_path)
    row = _get_connection(conn, connection_id)
    conn.close()
    if not row:
        return

    broker_id = str(row["broker"])
    try:
        connector = _get_connector_or_raise(broker_id)
        connector.unlink(db_path=db_path, connection_id=int(connection_id))
    except Exception:
        # Even if connector unlink fails, remove the DB row as a last resort.
        pass

    conn = _db(db_path)
    cur = conn.cursor()
    cur.execute("DELETE FROM broker_connections WHERE id=?", (int(connection_id),))
    conn.commit()
    conn.close()


def link_robinhood_connection(
    *,
    db_path: str,
    label: str,
    username: str,
    password: str,
    mfa_code: Optional[str] = None,
) -> tuple[bool, str]:
    """
    Step-7 placeholder link: creates/updates a robinhood connection record.
    Step 9 will implement proper login via robin_stocks and secure secret handling.
    """
    # If the real connector exists, use it.
    if RobinhoodConnector is not None:
        try:
            connector = _get_connector_or_raise("robinhood")
            connection_id = connector.link(
                db_path=db_path,
                label=label,
                username=username,
                password=password,
                mfa_code=mfa_code,
            )
            _update_connection_status(db_path, connection_id, "connected")
            return True, "Robinhood linked"
        except BrokerAuthError as e:
            return False, f"Robinhood auth error: {e}"
        except Exception as e:
            return False, f"Robinhood link failed: {e}"

    # Otherwise, store minimal record so UI can proceed while we build Step 9.
    conn = _db(db_path)
    cur = conn.cursor()
    now = _utc_ts()
    metadata = {"note": "Robinhood connector not installed yet (Step 9).", "username": username}

    # WARNING: This is intentionally minimal. We do NOT want to store the password long-term in plaintext.
    # Step 10 will introduce encryption-at-rest. Step 9 will store session/refresh tokens where possible.
    secrets = {"username": username, "password": password, "mfa_code": mfa_code or ""}

    cur.execute(
        """
        INSERT INTO broker_connections (broker, label, status, metadata_json, secrets_json, created_ts, updated_ts)
        VALUES (?,?,?,?,?,?,?)
        """,
        ("robinhood", label or "Robinhood", "needs_attention", json.dumps(metadata), json.dumps(secrets), now, now),
    )
    cid = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return True, f"Robinhood connection created (id {cid}). Connector comes in Step 9."


def _fetch_snapshots(
    db_path: str,
    *,
    connection_id: int = 0,
    force_refresh: bool = False,
) -> list[NormalizedPortfolioSnapshot]:
    cache_key = (db_path, int(connection_id or 0))
    now = _utc_ts()
    cached = _SNAPSHOT_CACHE.get(cache_key)
    if (not force_refresh) and cached and (now - int(cached.get("ts", 0))) < _SNAPSHOT_CACHE_TTL:
        return cached.get("snaps", [])

    conn = _db(db_path)
    rows = _list_connections(conn)
    conn.close()

    snapshots: list[NormalizedPortfolioSnapshot] = []
    for r in rows:
        cid = int(r["id"])
        if connection_id and cid != int(connection_id):
            continue

        broker_id = str(r["broker"])
        label = str(r["label"])
        status = str(r["status"])
        base_metadata = _safe_json(str(r["metadata_json"] or ""), default={})

        # Only attempt live fetch for "connected" (and legacy "ok"/empty), plus needs_auth recovery.
        if status not in ("connected", "ok", "", "needs_auth", "needs_attention", "error"):
            continue

        try:
            connector = _get_connector_or_raise(broker_id)
            snap = connector.portfolio_snapshot(db_path=db_path, connection_id=cid)
            # Ensure label/connection info is consistent
            snap.label = label
            snap.connection_id = cid
            snapshots.append(snap)
            if status != "connected":
                _update_connection_status(db_path, cid, "connected")
        except BrokerAuthError as e:
            status_code_raw = getattr(e, "status_code", 401)
            try:
                status_code = int(status_code_raw)
            except Exception:
                status_code = 401

            next_status = "needs_auth"
            if status_code >= 500:
                # Upstream/server failures are not auth failures.
                next_status = "error"
            elif status_code in (408, 429):
                next_status = "needs_attention"
            elif status_code not in (401, 403):
                next_status = "needs_attention"

            _update_connection_status(
                db_path,
                cid,
                next_status,
                metadata={
                    **(base_metadata or {}),
                    "error": str(e),
                    "broker": broker_id,
                    "label": label,
                    "status_code": status_code,
                },
            )
        except Exception as e:
            _update_connection_status(
                db_path,
                cid,
                "error",
                metadata={**(base_metadata or {}), "error": str(e), "broker": broker_id, "label": label},
            )

    if not connection_id:
        total_equity = _total_equity_from_snapshots(snapshots)
        _record_portfolio_equity(db_path, total_equity)

    _SNAPSHOT_CACHE[cache_key] = {"ts": now, "snaps": snapshots}
    return snapshots


def get_portfolio_summary_html(*, db_path: str) -> str:
    """
    Summary bar shown in layout.html. If no connected brokers return a helpful prompt.
    """
    conn = _db(db_path)
    rows = _list_connections(conn)
    conn.close()

    if not rows:
        return (
            "<div class='row'>"
            "<span class='small'>Brokers:</span> <span class='badge warn'>None linked</span>"
            "<a class='btn' href='/broker'>Link a broker</a>"
            "</div>"
        )

    # Try to pull connected snapshots and compute totals.
    snaps = _fetch_snapshots(db_path)
    if not snaps:
        # No connected snapshots; show statuses.
        connected = sum(1 for r in rows if str(r["status"]) == "connected")
        needs_auth = sum(1 for r in rows if str(r["status"]) in ("needs_auth", "needs_attention"))
        err = sum(1 for r in rows if str(r["status"]) == "error")
        return (
            "<div>"
            "<div class='row' style='justify-content:space-between'>"
            "<div class='row'>"
            "<span class='small'>Portfolio</span>"
            f"<span class='badge'>Connections {len(rows)}</span>"
            f"<span class='badge ok'>Connected {connected}</span>"
            f"<span class='badge warn'>Needs attention {needs_auth}</span>"
            f"<span class='badge bad'>Errors {err}</span>"
            "</div>"
            "<div class='row'>"
            "<a class='btn' href='/broker'>Open</a>"
            "</div>"
            "</div>"
            "</div>"
        )

    total_positions = 0
    total_accounts = 0

    for s in snaps:
        for a in s.accounts:
            total_accounts += 1
            total_positions += len(a.positions or [])

    return (
        "<div>"
        "<div class='row' style='justify-content:space-between'>"
        "<div class='row'>"
        "<span class='small'>Portfolio</span>"
        f"<span class='badge'>Brokers {len(snaps)}</span>"
        f"<span class='badge'>Accounts {total_accounts}</span>"
        f"<span class='badge'>Positions {total_positions}</span>"
        "</div>"
        "<div class='row'>"
        "<a class='btn' href='/broker'>Open</a>"
        "</div>"
        "</div>"
        "</div>"
    )


def get_portfolio_context_data(
    *,
    db_path: str,
    connection_id: int = 0,
    max_positions: int = 20,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """
    Return normalized portfolio data as JSON-friendly dicts for assistant context.
    Limits positions per account to max_positions (sorted by market value).
    """
    snaps = _fetch_snapshots(db_path, connection_id=connection_id, force_refresh=bool(force_refresh))
    out: list[dict[str, Any]] = []
    for snap in snaps:
        accounts_out: list[dict[str, Any]] = []
        for acc in snap.accounts:
            positions = list(acc.positions or [])
            positions_sorted = sorted(positions, key=_position_rank, reverse=True)
            positions_out: list[dict[str, Any]] = []
            for p in positions_sorted[:max_positions]:
                previous_close = _position_previous_close(p)
                day_pl = _position_day_pl(p)
                positions_out.append(
                    {
                        "symbol": p.symbol,
                        "quantity": p.quantity,
                        "average_price": p.average_price,
                        "market_price": p.market_price,
                        "market_value": p.market_value,
                        "unrealized_pl": p.unrealized_pl,
                        "asset_type": p.asset_type,
                        "price_multiplier": _position_price_multiplier(p),
                        "previous_close": previous_close,
                        "day_pl": day_pl,
                    }
                )
            accounts_out.append(
                {
                    "account_id": acc.account_id,
                    "account_type": acc.account_type,
                    "balances": acc.balances or {},
                    "positions_count": len(positions),
                    "positions_truncated": len(positions) > max_positions,
                    "positions": positions_out,
                }
            )
        out.append(
            {
                "broker": snap.broker,
                "label": snap.label,
                "connection_id": snap.connection_id,
                "accounts": accounts_out,
            }
        )
    return out


def get_portfolio_performance_context(*, db_path: str) -> dict[str, Any]:
    """
    Return portfolio performance summary from the equity log.
    """
    conn = _db(db_path)
    cur = conn.cursor()
    try:
        cur.execute("SELECT ts, total_equity FROM portfolio_equity_log ORDER BY ts DESC LIMIT 1")
        row = cur.fetchone()
    except Exception:
        conn.close()
        return {}
    conn.close()
    current_equity = float(row["total_equity"]) if row else None
    summary = _portfolio_performance_summary(db_path, current_equity)
    if row:
        summary["as_of"] = int(row["ts"])
    return summary


def get_portfolio_dashboard_html(*, db_path: str, connection_id: int = 0) -> str:
    """
    Dashboard panel used on /broker. If connection_id is provided, focus one connection.
    """
    conn = _db(db_path)
    rows = _list_connections(conn)
    conn.close()

    if not rows:
        return "<div class='small'>No broker connections yet. Add one on the left.</div>"

    # Always show connection status cards at the top
    status_cards: list[str] = []
    perf_html = _render_performance_badges(
        get_portfolio_performance_context(db_path=db_path),
        db_path=db_path,
    )
    if perf_html:
        status_cards.append(
            "<div class='card' style='margin-top:10px;'>"
            "<div class='small'>Portfolio Performance</div>"
            f"{perf_html}"
            "</div>"
        )
    for r in rows:
        cid = int(r["id"])
        if connection_id and cid != int(connection_id):
            continue
        broker_id = str(r["broker"])
        label = str(r["label"])
        status = str(r["status"])
        metadata = _safe_json(str(r["metadata_json"] or ""), default={})
        badge = "ok" if status == "connected" else ("warn" if status in ("needs_auth", "needs_attention") else "bad")
        debug_html = _render_debug_details(metadata)
        status_cards.append(
            "<div class='card' style='margin-top:10px'>"
            "<div class='row' style='justify-content:space-between'>"
            f"<div><b>{label}</b><div class='small'>{broker_id} · id {cid}</div></div>"
            f"<span class='badge {badge}'>{status}</span>"
            "</div>"
            "<div class='row' style='margin-top:10px'>"
            f"<a class='btn' href='/broker?connection_id={cid}'>Focus</a>"
            f"<a class='btn' href='/broker'>All</a>"
            "</div>"
            f"{debug_html}"
            "</div>"
        )

    snaps = _fetch_snapshots(db_path, connection_id=int(connection_id or 0))

    if not snaps:
        return "".join(status_cards) + "<div class='small' style='margin-top:12px'>No connected portfolios to display.</div>"

    blocks: list[str] = []
    blocks.extend(status_cards)

    for s in snaps:
        blocks.append(
            "<div class='card' style='margin-top:14px'>"
            "<div class='row' style='justify-content:space-between; align-items:flex-end;'>"
            f"<div><b>{s.label}</b><div class='small'>{s.broker} · connection {s.connection_id}</div></div>"
            "<div class='row'>"
            f"<a class='btn' href='/broker?connection_id={s.connection_id}'>Focus</a>"
            "</div>"
            "</div>"
        )

        for a in s.accounts:
            bal = a.balances or {}
            if not isinstance(bal, dict):
                bal = {}

            liq = _pick_balance(
                bal,
                ["liquidationValue", "accountValue", "extended_hours_equity", "equity", "netLiquidation", "totalAccountValue"],
            )
            cash = _pick_balance(bal, ["cashBalance", "cashAvailableForTrading", "availableFundsForTrading", "cash"])
            available_funds = _pick_balance(bal, ["availableFunds"])
            bp = _pick_balance(bal, ["buyingPower", "dayTradingBuyingPower", "availableFundsNonMarginableTrade"])
            available_funds_badge = ""
            if available_funds is not None:
                available_funds_badge = f"<span class='badge'>Available Funds {fmt_money(available_funds)}</span>"

            blocks.append(
                "<div class='card' style='margin-top:12px; padding:14px;'>"
                "<div class='row' style='justify-content:space-between'>"
                f"<div><b>Account {a.account_id}</b><div class='small'>{a.account_type or ''}</div></div>"
                "<div class='row'>"
                f"<span class='badge'>Net Liq {fmt_money(liq)}</span>"
                f"<span class='badge'>Cash {fmt_money(cash)}</span>"
                f"{available_funds_badge}"
                f"<span class='badge'>Margin BP {fmt_money(bp)}</span>"
                "</div>"
                "</div>"
            )

            pos = a.positions or []
            if pos:
                blocks.append(
                    "<div style='margin-top:10px'>"
                    "<div class='small'>Positions</div>"
                    "<table><thead><tr>"
                    "<th>Symbol</th><th>Qty</th><th>Avg</th><th>Price</th><th>Mkt Value</th><th>Unreal P/L</th>"
                    "</tr></thead><tbody>"
                )
                for p in pos:
                    pl_val = safe_float(p.unrealized_pl)
                    sym_class = _pl_class(pl_val)
                    sym_attr = f" class='{sym_class}'" if sym_class else ""
                    num_attr = f" class='{sym_class}'" if sym_class else ""
                    pl_attr = sym_attr
                    blocks.append(
                        "<tr>"
                        f"<td{sym_attr}><b>{p.symbol}</b><div class='small'>{p.asset_type}</div></td>"
                        f"<td{num_attr}>{fmt_num(p.quantity)}</td>"
                        f"<td{num_attr}>{fmt_money(p.average_price)}</td>"
                        f"<td{num_attr}>{fmt_money(p.market_price)}</td>"
                        f"<td{num_attr}>{fmt_money(p.market_value)}</td>"
                        f"<td{pl_attr}>{fmt_money(p.unrealized_pl)}</td>"
                        "</tr>"
                    )
                blocks.append("</tbody></table></div>")
            else:
                blocks.append("<div class='small' style='margin-top:10px'>No positions.</div>")

            blocks.append("</div>")  # end account card

        blocks.append("</div>")  # end broker card

    return "".join(blocks)

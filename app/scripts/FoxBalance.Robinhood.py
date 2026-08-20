#!/usr/bin/env python3
"""
FoxBalance.Robinhood.py (Web-App Compatible)

Based on the original standalone FoxBalance.rh.py.

Refactor goals:
- No interactive input() prompts
- Use Cryptid Exchange broker_connections to restore Robinhood session (pickle-based)
- Read runtime parameters from --params-json
- Preserve the original FoxBalance event-reactive loop + ranking + calculus + ATR trailing stop logic

Required CLI args (expected by the web app launcher):
  --run-dir <path>
  --params-json <path>
  --db-path <path_to_sqlite>
  --connection-id <int>   (Robinhood broker_connections.id)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import requests

try:
    import robin_stocks.robinhood as rh  # type: ignore
except Exception as e:
    raise RuntimeError(
        "robin_stocks is required for this script. Install with: pip install robin_stocks"
    ) from e

# -------------------------
# Ensure project imports work when executed as a script file
# -------------------------
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.brokers.robinhood_connector import (  # noqa: E402
    _pickle_debug_info,
    _resolve_pickle_config,
    _restore_session_from_pickle,
)
from app.brokers.robin_stocks_adapter import place_stock_order  # noqa: E402
from app.db import (  # noqa: E402
    get_broker_connection,
    read_connection_metadata,
    read_connection_secrets,
    set_broker_status,
)

# -----------------------------
# Policy knobs (ranking/eligibility)
# -----------------------------
PROFIT_LIQ_THRESHOLD_PCT = 0.00
INCLUDE_OVERWEIGHT_IN_LIQ_POOL = True

# -----------------------------
# Trading knobs (rebalance sizing) [legacy sizing kept, but not used for order qty in fixed-share mode]
# -----------------------------
PROFIT_TAKE_FRACTION_IF_UNDERWEIGHT = 0.25
MAX_CASH_USE_FRACTION_PER_BUY = 0.50

MIN_ORDER_DOLLARS = 1.00
MIN_QTY_EPS = 1e-6

# -----------------------------
# Whole-share trade sizing (PARAM-DRIVEN)
# -----------------------------
DEFAULT_SHARES_PER_TRADE_LOOP = 1

# -----------------------------
# Trailing stop ATR settings
# -----------------------------
ATR_PERIOD = 14
ATR_MULTIPLIER = 2.0
MIN_TRAIL_AMOUNT_USD = 0.01

# -----------------------------
# Mini chart sampling
# -----------------------------
CHART_POINTS = 90
HISTORICAL_BOUNDS = "extended"
MARKET_CODE = "XNYS"
MARKET_STATE_TTL = 60
_MARKET_STATE_CACHE: Dict[str, Any] = {"ts": 0, "state": "regular"}

# -----------------------------
# Calculus thresholds (keep logic + printing consistent)
# -----------------------------
BUY_RSI_LOW = 30.0
BUY_RSI_HIGH = 55.0
BUY_RSI_D_MIN = 1.0

SELL_RSI_MIN = 70.0
SELL_RSI_D_MAX = 0.5

# -----------------------------
# ANSI color helpers (terminal-only)
# -----------------------------
ANSI_RESET = "\033[0m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"

ENABLE_ANSI_COLORS = True

_ACCOUNT_NUMBER: Optional[str] = None


def colorize(text: str, color_code: str) -> str:
    if not ENABLE_ANSI_COLORS:
        return text
    return f"{color_code}{text}{ANSI_RESET}"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def mark_condition(label: str, ok: bool, color_code: str) -> str:
    status = "OK" if ok else "NO"
    base = f"{label:<40} : {status}"
    return colorize(base, color_code) if ok else base


# -----------------------------
# Robinhood session restore via DB + pickle path
# -----------------------------
def ensure_robinhood_session(db_path: str, connection_id: int) -> None:
    row = get_broker_connection(db_path, connection_id)
    if not row:
        raise RuntimeError(f"Robinhood connection_id {connection_id} not found in broker_connections.")

    broker = str(row["broker"])
    status = str(row["status"] or "")
    if broker != "robinhood":
        raise RuntimeError(f"connection_id {connection_id} is broker='{broker}', expected 'robinhood'.")

    if status != "connected":
        raise RuntimeError(
            f"Robinhood connection_id {connection_id} status='{status}'. Re-link Robinhood in Broker page."
        )

    meta = read_connection_metadata(row)
    secrets = read_connection_secrets(row, default={})

    _, _, pickle_file = _resolve_pickle_config(
        db_path=db_path, connection_id=int(connection_id), secrets=secrets
    )
    if not pickle_file.exists():
        debug = _pickle_debug_info(pickle_file)
        set_broker_status(
            db_path=db_path,
            connection_id=connection_id,
            status="needs_auth",
            metadata={**meta, "error": "Missing Robinhood session pickle.", "debug": debug},
        )
        raise RuntimeError("Robinhood session pickle missing; relink required.")

    restored = _restore_session_from_pickle(pickle_file, expires_in=86400, scope="internal", validate=True)
    if not restored:
        debug = _pickle_debug_info(pickle_file)
        set_broker_status(
            db_path=db_path,
            connection_id=connection_id,
            status="needs_auth",
            metadata={**meta, "error": "Failed to restore Robinhood session from pickle.", "debug": debug},
        )
        raise RuntimeError("Robinhood session restore failed; relink required.")


def login(db_path: str, connection_id: int, username: str = "", password: str = "", mfa_code: str = "") -> None:
    username = str(username or "")
    password = str(password or "")
    mfa_code = str(mfa_code or "")

    if username or password or mfa_code:
        print("Starting login process...")
        safe_call(lambda: rh.login(username, password, mfa_code=mfa_code), name="login")
        print("Connected to Robinhood.\n")
        return

    print("Starting login process (blank credentials; using existing session)...")
    ensure_robinhood_session(db_path, connection_id)
    print("Connected to Robinhood.\n")


# -----------------------------
# Rate-limit-safe helpers
# -----------------------------
def _status_code(exc: Exception) -> Optional[int]:
    return getattr(getattr(exc, "response", None), "status_code", None)


def safe_sleep(seconds: float) -> None:
    print(f"[RATE LIMIT] Cooling down for {seconds:.2f}s...")
    time.sleep(max(0.0, float(seconds)))


def safe_call(fn, retries: int = 3, backoff: float = 0.8, name: str = "call"):
    for attempt in range(retries):
        try:
            return fn()
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise


def safe_stock_quote(symbol: str) -> Dict:
    def _q():
        q = rh.stocks.get_stock_quote_by_symbol(symbol)
        if not q:
            raise ValueError(f"Empty quote for {symbol}")
        return q

    return safe_call(_q, name=f"quote({symbol})")


def safe_stock_historicals(
    symbol: str,
    interval: str,
    span: str,
    *,
    bounds: str = "regular",
) -> List[Dict]:
    def _h():
        if bounds:
            h = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span, bounds=bounds)
        else:
            h = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span)
        if not h:
            raise ValueError(f"Empty historicals for {symbol} ({interval}, {span})")
        return h

    return safe_call(_h, name=f"historicals({symbol},{interval},{span})")


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val in (None, "None"):
            return default
        return float(val)
    except Exception:
        return default


def _parse_iso_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _market_session_state() -> str:
    now = time.time()
    if now - float(_MARKET_STATE_CACHE.get("ts", 0)) < MARKET_STATE_TTL:
        return str(_MARKET_STATE_CACHE.get("state", "regular"))
    state = "regular"
    try:
        hours = rh.markets.get_market_today_hours(MARKET_CODE)
        if isinstance(hours, dict):
            now_dt = datetime.now(timezone.utc)
            reg_open = _parse_iso_ts(hours.get("opens_at"))
            reg_close = _parse_iso_ts(hours.get("closes_at"))
            ext_open = _parse_iso_ts(hours.get("extended_opens_at"))
            ext_close = _parse_iso_ts(hours.get("extended_closes_at"))
            if reg_open and reg_close and reg_open <= now_dt <= reg_close:
                state = "regular"
            elif ext_open and ext_close and ext_open <= now_dt <= ext_close:
                state = "extended"
            else:
                state = "closed"
    except Exception:
        state = "regular"
    _MARKET_STATE_CACHE["ts"] = now
    _MARKET_STATE_CACHE["state"] = state
    return state


def _price_from_quote(quote: dict, *, prefer_extended: bool) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    last = _safe_float(quote.get("last_trade_price"), default=0.0)
    last_ext = _safe_float(
        quote.get("last_extended_hours_trade_price")
        or quote.get("extended_hours_market_price"),
        default=0.0,
    )
    if prefer_extended:
        if last_ext > 0:
            return last_ext
        if last > 0:
            return last
    if last > 0:
        return last
    if last_ext > 0:
        return last_ext
    return None


def _mid_price(bid: float, ask: float, fallback: float) -> float:
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    if bid > 0:
        return bid
    if ask > 0:
        return ask
    return fallback


def _merge_historicals(base: List[Dict[str, Any]], extra: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not base:
        return extra
    if not extra:
        return base

    def _has_close(row: Dict[str, Any]) -> bool:
        val = row.get("close_price")
        return val not in (None, "None", "")

    merged: Dict[str, Dict[str, Any]] = {}
    for row in base:
        if not isinstance(row, dict):
            continue
        key = row.get("begins_at")
        if key:
            merged[str(key)] = row
    for row in extra:
        if not isinstance(row, dict):
            continue
        key = row.get("begins_at")
        if key:
            key = str(key)
            if key in merged:
                if _has_close(row) or not _has_close(merged[key]):
                    merged[key] = row
            else:
                merged[key] = row
    if not merged:
        return base
    return [merged[k] for k in sorted(merged.keys())]


def get_stock_historicals(symbol: str, interval: str, span: str) -> List[Dict]:
    data = safe_stock_historicals(symbol, interval=interval, span=span, bounds="regular")
    if not isinstance(data, list):
        raise RuntimeError(f"Failed to get historicals for {symbol}")
    try:
        extra = safe_stock_historicals(
            symbol,
            interval=interval,
            span="day",
            bounds=HISTORICAL_BOUNDS,
        )
        if isinstance(extra, list) and extra:
            data = _merge_historicals(data, extra)
    except Exception:
        pass
    return data


# -----------------------------
# DreadFox calculus (ported)
# -----------------------------
def calculate_moving_average(prices: List[float], window_size: int) -> Optional[float]:
    if len(prices) < window_size:
        return None
    return float(np.mean(prices[-window_size:]))


def _ma_series(prices: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(prices)
    if len(prices) < window:
        return out
    window_sum = float(sum(prices[:window]))
    out[window - 1] = window_sum / window
    for i in range(window, len(prices)):
        window_sum += float(prices[i]) - float(prices[i - window])
        out[i] = window_sum / window
    return out


def _build_chart_series(prices: List[float], max_points: int = CHART_POINTS) -> Dict[str, List[Optional[float]]]:
    if not prices:
        return {}
    ma20_full = _ma_series(prices, 20)
    ma78_full = _ma_series(prices, 78)
    ma150_full = _ma_series(prices, 190)
    if max_points > 0 and len(prices) > max_points:
        offset = len(prices) - max_points
        return {
            "price": [float(p) for p in prices[-max_points:]],
            "ma20": ma20_full[offset:],
            "ma78": ma78_full[offset:],
            "ma150": ma150_full[offset:],
        }
    return {
        "price": [float(p) for p in prices],
        "ma20": ma20_full,
        "ma78": ma78_full,
        "ma150": ma150_full,
    }


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None

    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = float(np.sum(gains[:period]) / period)
    avg_loss = float(np.sum(losses[:period]) / period)

    for i in range(period, len(deltas)):
        avg_gain = ((avg_gain * (period - 1)) + float(gains[i])) / period
        avg_loss = ((avg_loss * (period - 1)) + float(losses[i])) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def calculate_rsi_and_derivative(prices: List[float], period: int = 14) -> Tuple[Optional[float], Optional[float]]:
    if len(prices) < period + 3:
        return None, None

    rsi_t2 = calculate_rsi(prices[:-2], period)
    rsi_t1 = calculate_rsi(prices[:-1], period)
    rsi_t0 = calculate_rsi(prices, period)

    if rsi_t0 is None or rsi_t1 is None or rsi_t2 is None:
        return rsi_t0, None

    d_rsi = (rsi_t0 - rsi_t2) / 2.0
    return rsi_t0, float(d_rsi)


def dreadfox_signal(
    prices: List[float],
    current_price: float,
    avg_buy_price: float,
    held_qty: float,
) -> Tuple[str, Dict[str, Optional[float]]]:
    ma20 = calculate_moving_average(prices, 20)
    ma78 = calculate_moving_average(prices, 78)
    ma150 = calculate_moving_average(prices, 190)

    rsi, rsi_d = calculate_rsi_and_derivative(prices, period=14)

    meta: Dict[str, Optional[float]] = {
        "ma20": ma20,
        "ma78": ma78,
        "ma150": ma150,
        "rsi": rsi,
        "rsi_d": rsi_d,
    }

    if None in (ma20, ma78, ma150, rsi, rsi_d):
        return "HOLD", meta

    buy = (
        current_price > ma20
        and current_price < ma78
        and current_price < ma150
        and (BUY_RSI_LOW < float(rsi) < BUY_RSI_HIGH)
        and (float(rsi_d) > BUY_RSI_D_MIN)
    )

    sell = (
        held_qty > 0
        and current_price > ma20
        and current_price > ma78
        and current_price > ma150
        and (float(rsi_d) < SELL_RSI_D_MAX)
        and (float(rsi) > SELL_RSI_MIN)
        and (avg_buy_price > 0 and current_price > avg_buy_price)
    )

    if buy and not sell:
        return "BUY", meta
    if sell and not buy:
        return "SELL", meta
    return "HOLD", meta


# -----------------------------
# ATR
# -----------------------------
def extract_hlc(hist: List[Dict]) -> Tuple[List[float], List[float], List[float]]:
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for row in hist:
        hp = row.get("high_price")
        lp = row.get("low_price")
        cp = row.get("close_price")
        if hp in (None, "None") or lp in (None, "None") or cp in (None, "None"):
            continue
        highs.append(float(hp))
        lows.append(float(lp))
        closes.append(float(cp))
    return highs, lows, closes


def calculate_atr_wilder(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> Optional[float]:
    if len(highs) != len(lows) or len(lows) != len(closes):
        return None
    n = len(closes)
    if n < period + 1:
        return None

    trs: List[float] = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(float(tr))

    if len(trs) < period:
        return None

    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]:
        atr = ((atr * (period - 1)) + float(tr)) / period

    return float(atr)


# -----------------------------
# Timeframe config
# -----------------------------
TIMEFRAMES = {
    "5m": {"interval": "5minute", "span": "week", "window": 52, "label": "52x5m"},
    "10m": {"interval": "10minute", "span": "week", "window": 52, "label": "52x10m"},
    "1h": {"interval": "hour", "span": "3month", "window": 52, "label": "52-hour"},
    "1d": {"interval": "day", "span": "year", "window": 52, "label": "52-day"},
}


def _resolve_timeframe(timeframe_key: str, session_state: str) -> Tuple[str, str]:
    tf = TIMEFRAMES[timeframe_key]
    interval = tf["interval"]
    span = tf["span"]
    if session_state == "extended" and timeframe_key in ("1h", "1d"):
        interval = "10minute"
        span = "week"
    return str(interval), str(span)


def window_pos(prices: List[float], current_price: float, window: int) -> float:
    if len(prices) < max(10, window):
        return 0.0
    recent = prices[-window:]
    low = min(recent)
    high = max(recent)
    if high <= low:
        return 0.0
    x = (current_price - low) / (high - low)
    return float(max(0.0, min(1.0, x)))


# -----------------------------
# Portfolio model
# -----------------------------
@dataclass
class PositionRow:
    symbol: str
    quantity: float
    avg_buy_price: float
    current_price: float
    bid_price: float
    ask_price: float
    market_value: float
    pnl_percent: float
    alloc_percent: float
    delta_percent: float
    window_pos: float
    calc_signal: str
    calc_meta: Dict[str, Optional[float]]
    atr: Optional[float]
    chart: Dict[str, List[Optional[float]]]


def build_status_rows(
    rows: List[PositionRow],
    *,
    top_liq: Optional[PositionRow],
    top_acq: Optional[PositionRow],
) -> List[Dict[str, Any]]:
    top_liq_sym = top_liq.symbol if top_liq is not None else ""
    top_acq_sym = top_acq.symbol if top_acq is not None else ""
    out: List[Dict[str, Any]] = []
    for r in rows:
        role = "LIQ" if r.symbol == top_liq_sym else ("ACQ" if r.symbol == top_acq_sym else "")
        out.append(
            {
                "symbol": r.symbol,
                "role": role,
                "signal": r.calc_signal,
                "price": r.current_price,
                "avg_buy": r.avg_buy_price,
                "qty": r.quantity,
                "market_value": r.market_value,
                "pnl_pct": r.pnl_percent,
                "alloc_pct": r.alloc_percent,
                "delta_pct": r.delta_percent,
                "window_pos": r.window_pos,
                "ma20": r.calc_meta.get("ma20"),
                "ma78": r.calc_meta.get("ma78"),
                "ma150": r.calc_meta.get("ma150"),
                "rsi": r.calc_meta.get("rsi"),
                "rsi_d": r.calc_meta.get("rsi_d"),
                "atr": r.atr,
                "chart": r.chart,
            }
        )
    return out


def get_portfolio_equity() -> float:
    profile = safe_call(rh.profiles.load_portfolio_profile, name="load_portfolio_profile")
    return float(profile["equity"])


def get_reported_cash_field() -> float:
    acct = safe_call(rh.profiles.load_account_profile, name="load_account_profile")
    return float(acct.get("cash", 0.0) or 0.0)


def _resolve_account_number() -> Optional[str]:
    global _ACCOUNT_NUMBER
    if _ACCOUNT_NUMBER:
        return _ACCOUNT_NUMBER
    try:
        acct = safe_call(rh.profiles.load_account_profile, name="load_account_profile")
    except Exception:
        return None
    if isinstance(acct, dict):
        acct_num = acct.get("account_number") or acct.get("rhs_account_number")
        if acct_num:
            _ACCOUNT_NUMBER = str(acct_num)
            return _ACCOUNT_NUMBER
    return None


def get_open_stock_positions() -> List[Dict]:
    account_number = _resolve_account_number()
    if not account_number:
        raise RuntimeError("Robinhood account_number missing; cannot fetch positions.")
    return safe_call(
        lambda: rh.account.get_open_stock_positions(account_number=account_number),
        name="get_open_stock_positions",
    )


def get_symbols_and_qty_from_positions(positions: List[Dict]) -> Tuple[List[str], Dict[str, float], Dict[str, float]]:
    symbols: List[str] = []
    qty_by: Dict[str, float] = {}
    avg_by: Dict[str, float] = {}

    for p in positions:
        qty = float(p.get("quantity", 0) or 0)
        if qty <= 0:
            continue

        inst_url = p.get("instrument")
        if not inst_url:
            continue

        inst = safe_call(lambda: rh.account.get_instrument_by_url(inst_url), name="get_instrument_by_url")
        sym = (inst or {}).get("symbol")
        if not sym:
            continue

        avg_buy = float(p.get("average_buy_price", 0) or 0)

        symbols.append(sym)
        qty_by[sym] = qty_by.get(sym, 0.0) + qty
        avg_by[sym] = avg_buy

    uniq = sorted(set(symbols))
    return uniq, qty_by, avg_by


def get_position_rows(
    portfolio_equity: float,
    target_slice: float,
    timeframe_key: str,
    session_state: str,
) -> List[PositionRow]:
    tf = TIMEFRAMES[timeframe_key]
    interval, span = _resolve_timeframe(timeframe_key, session_state)
    win = tf["window"]

    positions = get_open_stock_positions()
    symbols, qty_by, avg_by = get_symbols_and_qty_from_positions(positions)

    rows: List[PositionRow] = []
    for sym in symbols:
        try:
            qty = float(qty_by[sym])
            avg_buy = float(avg_by.get(sym, 0.0))

            quote = safe_stock_quote(sym)
            current = _price_from_quote(quote, prefer_extended=session_state == "extended")
            if current is None:
                raise RuntimeError(f"Missing price for {sym}")
            bid_price = _safe_float(quote.get("bid_price"), default=0.0)
            ask_price = _safe_float(quote.get("ask_price"), default=0.0)

            hist_interval = interval
            hist_span = span
            hist = get_stock_historicals(sym, interval=hist_interval, span=hist_span)
            highs, lows, closes_hist = extract_hlc(hist)
            if session_state == "extended" and timeframe_key in ("1h", "1d"):
                if (len(closes_hist) + 1) < 190:
                    hist_interval = "5minute"
                    hist_span = "week"
                    hist = get_stock_historicals(sym, interval=hist_interval, span=hist_span)
                    highs, lows, closes_hist = extract_hlc(hist)

            closes_for_calc = list(closes_hist)
            if closes_for_calc and closes_for_calc[-1] != current:
                closes_for_calc.append(current)

            sig, meta = dreadfox_signal(
                closes_for_calc, current_price=current, avg_buy_price=avg_buy, held_qty=qty
            )

            mv = qty * current
            alloc = (mv / portfolio_equity) * 100 if portfolio_equity > 0 else 0.0
            delta = alloc - target_slice
            pnl = ((current - avg_buy) / avg_buy) * 100 if avg_buy > 0 else 0.0
            wp = window_pos(closes_for_calc, current, window=win)

            atr = calculate_atr_wilder(highs, lows, closes_hist, period=ATR_PERIOD)
            chart = _build_chart_series(closes_for_calc)

            rows.append(
                PositionRow(
                    symbol=sym,
                    quantity=qty,
                    avg_buy_price=avg_buy,
                    current_price=current,
                    bid_price=bid_price,
                    ask_price=ask_price,
                    market_value=mv,
                    pnl_percent=pnl,
                    alloc_percent=alloc,
                    delta_percent=delta,
                    window_pos=wp,
                    calc_signal=sig,
                    calc_meta=meta,
                    atr=atr,
                    chart=chart,
                )
            )
        except Exception as e:
            print(f"[WARN] Failed processing {sym}: {e}")

    rows.sort(key=lambda r: r.alloc_percent, reverse=True)
    return rows


# -----------------------------
# Options market value (best-effort)
# -----------------------------
def get_options_market_value_total() -> float:
    try:
        positions = safe_call(rh.options.get_open_option_positions, name="get_open_option_positions")
        total = 0.0
        for p in positions:
            qty = float(p.get("quantity", 0) or 0)
            if qty <= 0:
                continue

            mv = None
            for k in ("market_value", "trade_value", "intraday_market_value", "average_price"):
                v = p.get(k)
                if v not in (None, "None"):
                    try:
                        mv = float(v)
                        break
                    except Exception:
                        pass

            if mv is None:
                continue

            total += mv
        return float(total)
    except Exception as e:
        print(f"[WARN] Options value unavailable (treated as $0.00): {e}")
        return 0.0


# -----------------------------
# Pretty printing
# -----------------------------
def _fmt_money(x: float) -> str:
    return f"${x:,.2f}"


def _fmt_price(x: float) -> str:
    return f"${x:,.2f}"


def _fmt_pct(x: float) -> str:
    return f"{x:+.2f}%"


def print_kv_table(rows: List[Tuple[str, str]], title: str = "") -> None:
    if title:
        print(title)
    w = max((len(k) for k, _ in rows), default=10)
    for k, v in rows:
        print(f"  {k:<{w}} : {v}")
    print()


def print_positions_table(rows: List[Tuple[PositionRow, float]], target_slice: float, label: str, title: str) -> None:
    safe_label = str(label or "window")
    if label is None:
        print("[WARN] Missing timeframe label; defaulting to 'window'.")
    print(title)
    header = (
        f"{'SYM':<6}"
        f"{'PX':>10} "
        f"{'QTY':>12} "
        f"{'MV':>12} "
        f"{'ALLOC%':>8} "
        f"{'TGT%':>7} "
        f"{'DELTA':>8} "
        f"{'PNL%':>8} "
        f"{(safe_label + '_pos'):>10} "
        f"{'ATR':>8} "
        f"{'RANK':>9} "
        f"{'SIG':>6}"
    )
    print(header)
    print("-" * len(header))

    if not rows:
        print("(none)\n")
        return

    for r, score in rows:
        atr_s = "--" if r.atr is None else f"{r.atr:.2f}"
        print(
            f"{r.symbol:<6}"
            f"{_fmt_price(r.current_price):>10} "
            f"{r.quantity:>12.6f} "
            f"{_fmt_money(r.market_value):>12} "
            f"{r.alloc_percent:>8.2f} "
            f"{target_slice:>7.2f} "
            f"{r.delta_percent:>8.2f} "
            f"{r.pnl_percent:>+8.2f} "
            f"{r.window_pos:>10.3f} "
            f"{atr_s:>8} "
            f"{score:>9.3f} "
            f"{r.calc_signal:>6}"
        )
    print()


def print_gating_table(liq: Optional[PositionRow], acq: Optional[PositionRow]) -> None:
    print("[CALCULUS GATING] (top-ranked only)")
    header = (
        f"{'TYPE':<4} {'SYM':<6} {'SIG':<5} {'NEED':<5} {'PASS':<5} "
        f"{'MA20':>10} {'MA78':>10} {'MA190':>10} {'RSI':>7} {'RSI_d':>7} {'ATR':>7}"
    )
    print(header)
    print("-" * len(header))

    def _num(x: Optional[float], width: int, prec: int) -> str:
        if x is None:
            return f"{'--':>{width}}"
        return f"{x:>{width}.{prec}f}"

    def row_line(tp: str, r: PositionRow, need: str) -> str:
        ok = (r.calc_signal == need)
        ma20 = r.calc_meta.get("ma20")
        ma78 = r.calc_meta.get("ma78")
        ma150 = r.calc_meta.get("ma150")
        rsi = r.calc_meta.get("rsi")
        rsi_d = r.calc_meta.get("rsi_d")

        return (
            f"{tp:<4} {r.symbol:<6} {r.calc_signal:<5} {need:<5} {('YES' if ok else 'NO'):<5} "
            f"{_num(ma20, 10, 2)} {_num(ma78, 10, 2)} {_num(ma150, 10, 2)} "
            f"{_num(rsi, 7, 2)} {_num(rsi_d, 7, 2)} {_num(r.atr, 7, 2)}"
        )

    if liq is None:
        print("LIQ  (none)")
    else:
        print(row_line("LIQ", liq, "SELL"))

    if acq is None:
        print("ACQ  (none)")
    else:
        print(row_line("ACQ", acq, "BUY"))
    print()


# -----------------------------
# Trailing amount formatting (CENT-PRECISE)
# -----------------------------
def round_to_cent(x: float) -> float:
    return float(f"{x:.2f}")


def atr_based_trail_amount(r: PositionRow) -> Optional[float]:
    if r.atr is None or r.atr <= 0:
        return None

    amt = ATR_MULTIPLIER * r.atr
    if amt < MIN_TRAIL_AMOUNT_USD:
        amt = MIN_TRAIL_AMOUNT_USD

    amt = round_to_cent(amt)
    if amt < MIN_TRAIL_AMOUNT_USD:
        amt = round_to_cent(MIN_TRAIL_AMOUNT_USD)

    return amt


# -----------------------------
# Condition explanations
# -----------------------------
def _safe_gt(a: Optional[float], b: Optional[float]) -> bool:
    return (a is not None) and (b is not None) and (a > b)


def _safe_lt(a: Optional[float], b: Optional[float]) -> bool:
    return (a is not None) and (b is not None) and (a < b)


def _safe_between(x: Optional[float], lo: float, hi: float) -> bool:
    return (x is not None) and (lo < x < hi)


def print_calculus_conditions(tp: str, r: PositionRow) -> None:
    ma20 = r.calc_meta.get("ma20")
    ma78 = r.calc_meta.get("ma78")
    ma150 = r.calc_meta.get("ma150")
    rsi = r.calc_meta.get("rsi")
    rsi_d = r.calc_meta.get("rsi_d")
    cp = r.current_price
    avg = r.avg_buy_price
    qty = r.quantity

    if tp == "LIQ":
        color = ANSI_RED
        print("[CALCULUS CONDITIONS] LIQ (SELL rules) - red means condition satisfied")
        conds = [
            ("held_qty > 0", qty > 0),
            ("current_price > MA20", _safe_gt(cp, ma20)),
            ("current_price > MA78", _safe_gt(cp, ma78)),
            ("current_price > MA150", _safe_gt(cp, ma150)),
            (f"RSI > {SELL_RSI_MIN:.0f}", (rsi is not None and rsi > SELL_RSI_MIN)),
            (f"RSI_d < {SELL_RSI_D_MAX:.0f}", (rsi_d is not None and rsi_d < SELL_RSI_D_MAX)),
            ("current_price > avg_buy_price", (avg is not None and avg > 0 and cp > avg)),
        ]
    else:
        color = ANSI_GREEN
        print("[CALCULUS CONDITIONS] ACQ (BUY rules) - green means condition satisfied")
        conds = [
            ("current_price > MA20", _safe_gt(cp, ma20)),
            ("current_price < MA78", _safe_lt(cp, ma78)),
            ("current_price < MA150", _safe_lt(cp, ma150)),
            (f"{BUY_RSI_LOW:.0f} < RSI < {BUY_RSI_HIGH:.0f}", _safe_between(rsi, BUY_RSI_LOW, BUY_RSI_HIGH)),
            (f"RSI_d > {BUY_RSI_D_MIN:.0f}", (rsi_d is not None and rsi_d > BUY_RSI_D_MIN)),
        ]

    for label, ok in conds:
        print("  " + mark_condition(label, ok, color))
    print()


def print_target_details(tp: str, r: PositionRow, target_slice: float, effective_cash: float) -> None:
    ma20 = r.calc_meta.get("ma20")
    ma78 = r.calc_meta.get("ma78")
    ma150 = r.calc_meta.get("ma150")
    rsi = r.calc_meta.get("rsi")
    rsi_d = r.calc_meta.get("rsi_d")

    trail_amt = atr_based_trail_amount(r) if tp == "LIQ" else None

    def n2(x: Optional[float]) -> str:
        return "--" if x is None else f"{x:.2f}"

    print(f"[TARGET DETAILS] {tp} -> {r.symbol}")
    print(f"  current_price         : {_fmt_price(r.current_price)}")
    print(f"  qty                  : {r.quantity:.6f}")
    print(f"  mv                   : {_fmt_money(r.market_value)}")
    print(f"  alloc% / tgt%         : {r.alloc_percent:.2f}% / {target_slice:.2f}%  (delta {r.delta_percent:+.2f}%)")
    print(f"  pnl%                 : {r.pnl_percent:+.2f}%")
    print(f"  window_pos           : {r.window_pos:.3f}")
    print(f"  calculus signal      : {r.calc_signal}")
    print(f"  MA20 / MA78 / MA190  : {n2(ma20)} / {n2(ma78)} / {n2(ma150)}")
    print(f"  RSI / RSI_d          : {n2(rsi)} / {n2(rsi_d)}")
    print(f"  ATR({ATR_PERIOD})            : {n2(r.atr)}")
    if tp == "LIQ":
        print(f"  trailAmount (2xATR)  : {('--' if trail_amt is None else f'${trail_amt:.2f}')}")
    else:
        print(f"  effective_cash       : {_fmt_money(effective_cash)}")
    print()

    print_calculus_conditions(tp, r)


# -----------------------------
# Ranking rules
# -----------------------------
def is_profit_liquidation_candidate(r: PositionRow) -> bool:
    return r.pnl_percent > PROFIT_LIQ_THRESHOLD_PCT


def is_overweight_liquidation_candidate(r: PositionRow) -> bool:
    return r.delta_percent > 0.0 and r.pnl_percent > PROFIT_LIQ_THRESHOLD_PCT


def liquidation_rank_score(r: PositionRow) -> float:
    return float((max(r.pnl_percent, 0.0) * 10000.0) + r.delta_percent)


def acquisition_rank_score(r: PositionRow) -> float:
    return float((1.0 - r.window_pos) + (abs(r.delta_percent) / 100.0))


# -----------------------------
# Whole-share execution checks
# -----------------------------
def clamp_whole_share_qty(requested_shares: int, available_shares: float) -> Optional[float]:
    if requested_shares < 1:
        return None
    if available_shares < float(requested_shares):
        return None
    return float(requested_shares)


def can_afford_whole_share_buy(requested_shares: int, effective_cash: float, price: float) -> bool:
    if requested_shares < 1 or price <= 0:
        return False
    return effective_cash >= (float(requested_shares) * price)


def _order_success(resp: Any) -> bool:
    if hasattr(resp, "accepted") and hasattr(resp, "submitted"):
        return bool(resp.accepted and resp.submitted and not getattr(resp, "blocked", False))
    if isinstance(resp, dict):
        state = str(resp.get("state") or "").lower()
        if state:
            return state in ("queued", "confirmed", "filled")
        return bool(resp.get("id"))
    return bool(resp)


def _record_trade(
    stats: Dict[str, Any],
    *,
    side: str,
    qty: float,
    price: float,
    avg_buy_price: float,
) -> None:
    stats["trades"] = int(stats.get("trades", 0)) + 1
    if side == "sell" and avg_buy_price > 0 and qty > 0:
        profit = (price - avg_buy_price) * qty
        if profit > 0:
            stats["pnl"] = float(stats.get("pnl", 0.0)) + profit


# -----------------------------
# Trading execution
# -----------------------------
def place_market_buy(symbol: str, qty: float) -> Dict:
    return safe_call(
        lambda: place_stock_order(
            symbol=symbol,
            side="buy",
            order_type="market",
            quantity=qty,
            timeInForce="gfd",
        ),
        name=f"adapter.order_buy_market({symbol})",
    )


def place_trailing_stop_sell(symbol: str, qty: float, trail_amount_usd: float) -> Dict:
    return safe_call(
        lambda: place_stock_order(
            symbol=symbol,
            side="sell",
            order_type="trailing_stop",
            quantity=qty,
            trailAmount=trail_amount_usd,
            trailType="amount",
            timeInForce="gtc",
        ),
        name=f"adapter.order_sell_trailing_stop({symbol})",
    )


# -----------------------------
# Full analysis + trade cycle
# -----------------------------
def fox_balance_cycle(
    timeframe_key: str,
    top_n: int,
    trading_enabled: bool,
    shares_per_loop: int,
    session_state: str,
    *,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
    status_reason: str = "",
) -> Dict[str, float]:
    tf = TIMEFRAMES[timeframe_key]
    interval, span = _resolve_timeframe(timeframe_key, session_state)
    label = str(tf.get("label") or tf.get("window") or "window")

    equity = get_portfolio_equity()
    reported_cash = get_reported_cash_field()

    rows = get_position_rows(
        equity,
        target_slice=0.0,
        timeframe_key=timeframe_key,
        session_state=session_state,
    )

    slices = len(rows) + 1
    target_slice = (100.0 / slices) if slices > 0 else 0.0
    for r in rows:
        r.delta_percent = r.alloc_percent - target_slice

    stock_total = sum(r.market_value for r in rows)
    options_total = get_options_market_value_total()

    effective_cash = equity - stock_total - options_total
    if -1.0 < effective_cash < 0.0:
        effective_cash = 0.0

    cash_alloc = (effective_cash / equity) * 100 if equity > 0 else 0.0
    cash_delta = cash_alloc - target_slice

    liq_pool: List[PositionRow] = []
    for r in rows:
        if is_profit_liquidation_candidate(r) or (
            INCLUDE_OVERWEIGHT_IN_LIQ_POOL and is_overweight_liquidation_candidate(r)
        ):
            liq_pool.append(r)

    liq_symbols = {r.symbol for r in liq_pool}
    acq_pool = [
        r
        for r in rows
        if (r.symbol not in liq_symbols) and (r.pnl_percent <= PROFIT_LIQ_THRESHOLD_PCT)
    ]

    liq_ranked = sorted(
        [(r, liquidation_rank_score(r)) for r in liq_pool],
        key=lambda t: (t[0].pnl_percent, t[0].delta_percent),
        reverse=True,
    )
    acq_ranked = sorted(
        [(r, acquisition_rank_score(r)) for r in acq_pool],
        key=lambda t: t[1],
        reverse=True,
    )

    print_kv_table(
        [
            ("Timeframe", f"{timeframe_key} (interval={interval}, span={span}, window={label})"),
            ("Broker-reported equity", _fmt_money(equity)),
            ("Reported cash field (ref only)", _fmt_money(reported_cash)),
            ("Stock market value total", _fmt_money(stock_total)),
            ("Options market value total", _fmt_money(options_total)),
            ("Effective cash (equity - stocks - options)", _fmt_money(effective_cash)),
            ("Effective cash alloc", f"{cash_alloc:.2f}%"),
            ("Target slice", f"{target_slice:.4f}%"),
            ("Cash delta", _fmt_pct(cash_delta)),
            ("Held tickers (N)", str(len(rows))),
            ("Slices", f"{len(rows)} tickers + 1 cash = {len(rows) + 1}"),
            ("Ranking rule (LIQ)", "PnL>0 enters LIQ pool; LIQ ranked by PNL% then DELTA%"),
            ("Ranking rule (ACQ)", "PnL<=0 enters ACQ pool; ranked by 52-window lows then delta magnitude"),
            ("Options policy", "Options included in value math only; not included in slice/ranking pools"),
            ("Trailing stop policy", f"trailAmount = {ATR_MULTIPLIER:.1f}x ATR({ATR_PERIOD}), rounded to cents, floor=${MIN_TRAIL_AMOUNT_USD:.2f}"),
            ("Trading enabled", "YES" if trading_enabled else "NO"),
            ("ANSI colors", "ON" if ENABLE_ANSI_COLORS else "OFF"),
            ("SELL RSI threshold", f">{SELL_RSI_MIN:.0f} (printing matches logic)"),
            ("Shares per trade loop", f"{shares_per_loop} (whole shares, user-configured)"),
        ],
        title="[PORTFOLIO SUMMARY]",
    )

    print_positions_table(
        liq_ranked[:top_n],
        target_slice,
        label,
        "[LIQUIDATION CANDIDATES] (profit-taking, ranked by PNL% then DELTA%)",
    )
    print_positions_table(
        acq_ranked[:top_n],
        target_slice,
        label,
        "[ACQUISITION CANDIDATES] (loss pool, ranked by 52-low bias)",
    )

    top_liq = liq_ranked[0][0] if liq_ranked else None
    top_acq = acq_ranked[0][0] if acq_ranked else None

    print_gating_table(top_liq, top_acq)

    if top_liq is not None:
        print_target_details("LIQ", top_liq, target_slice=target_slice, effective_cash=effective_cash)
    if top_acq is not None:
        print_target_details("ACQ", top_acq, target_slice=target_slice, effective_cash=effective_cash)

    if trading_enabled:
        if session_state == "closed":
            print("Market closed; skipping all orders.\n")
        else:
            if session_state == "extended":
                print("Extended hours: using limit orders at bid/ask midpoint.")
            if top_liq is not None and top_liq.calc_signal == "SELL":
                sell_qty = clamp_whole_share_qty(shares_per_loop, top_liq.quantity)
                if sell_qty is None:
                    print(
                        f"[TRADE] LIQ passed calculus, but insufficient shares for requested sell "
                        f"(need {shares_per_loop}, have {top_liq.quantity:.6f}); no order placed.\n"
                    )
                elif session_state == "extended":
                    limit_price = _mid_price(top_liq.bid_price, top_liq.ask_price, top_liq.current_price)
                    print(
                        f"[TRADE] LIQ SELL (limit) {top_liq.symbol} "
                        f"qty={sell_qty:.0f} limit=${limit_price:.2f}"
                    )
                    resp = safe_call(
                        lambda: place_stock_order(
                            symbol=top_liq.symbol,
                            side="sell",
                            order_type="limit",
                            quantity=sell_qty,
                            limitPrice=float(limit_price),
                            timeInForce="gtc",
                            extendedHours=True,
                        ),
                        name=f"adapter.order_sell_limit({top_liq.symbol})",
                    )
                    print(f"[TRADE] Response: state={resp.get('state')}, id={resp.get('id')}\n")
                    if trade_stats is not None and _order_success(resp):
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(sell_qty),
                            price=top_liq.current_price,
                            avg_buy_price=top_liq.avg_buy_price,
                        )
                else:
                    trail_amt = atr_based_trail_amount(top_liq)
                    if trail_amt is None:
                        print(
                            "[TRADE] LIQ passed calculus, but ATR unavailable -> cannot compute trailAmount; no order placed.\n"
                        )
                    else:
                        print(
                            f"[TRADE] LIQ SELL (trailing stop) {top_liq.symbol} "
                            f"qty={sell_qty:.0f} trail=${trail_amt:.2f}"
                        )
                        resp = place_trailing_stop_sell(top_liq.symbol, sell_qty, trail_amt)
                        print(f"[TRADE] Response: state={resp.get('state')}, id={resp.get('id')}\n")
                        if trade_stats is not None and _order_success(resp):
                            _record_trade(
                                trade_stats,
                                side="sell",
                                qty=float(sell_qty),
                                price=top_liq.current_price,
                                avg_buy_price=top_liq.avg_buy_price,
                            )
            else:
                if top_liq is None:
                    print("[TRADE] No LIQ candidate.\n")
                else:
                    print("[TRADE] Top LIQ did not pass SELL calculus; no order placed.\n")

            if top_acq is not None and top_acq.calc_signal == "BUY":
                if not can_afford_whole_share_buy(shares_per_loop, effective_cash, top_acq.current_price):
                    need = float(shares_per_loop) * top_acq.current_price
                    print(
                        f"[TRADE] ACQ passed calculus, but insufficient effective_cash for requested buy "
                        f"(need {_fmt_money(need)}, have {_fmt_money(effective_cash)}); no order placed.\n"
                    )
                else:
                    buy_qty = float(shares_per_loop)
                    if session_state == "extended":
                        limit_price = _mid_price(top_acq.bid_price, top_acq.ask_price, top_acq.current_price)
                        print(
                            f"[TRADE] ACQ BUY (limit) {top_acq.symbol} "
                            f"qty={buy_qty:.0f} limit=${limit_price:.2f} (user-configured whole-share mode)"
                        )
                        resp = safe_call(
                            lambda: place_stock_order(
                                symbol=top_acq.symbol,
                                side="buy",
                                order_type="limit",
                                quantity=buy_qty,
                                limitPrice=float(limit_price),
                                timeInForce="gfd",
                                extendedHours=True,
                            ),
                            name=f"adapter.order_buy_limit({top_acq.symbol})",
                        )
                    else:
                        print(
                            f"[TRADE] ACQ BUY (market) {top_acq.symbol} "
                            f"qty={buy_qty:.0f} (user-configured whole-share mode)"
                        )
                        resp = place_market_buy(top_acq.symbol, buy_qty)
                    print(f"[TRADE] Response: state={resp.get('state')}, id={resp.get('id')}\n")
                    if trade_stats is not None and _order_success(resp):
                        _record_trade(
                            trade_stats,
                            side="buy",
                            qty=float(buy_qty),
                            price=top_acq.current_price,
                            avg_buy_price=0.0,
                        )
            else:
                if top_acq is None:
                    print("[TRADE] No ACQ candidate.\n")
                else:
                    print("[TRADE] Top ACQ did not pass BUY calculus; no order placed.\n")
    else:
        print("Trading disabled (analysis mode). No orders placed.\n")

    if status_writer is not None:
        try:
            status_writer(
                {
                    "phase": "cycle",
                    "timeframe": timeframe_key,
                    "trading_enabled": bool(trading_enabled),
                    "reason": status_reason,
                    "equity": equity,
                    "effective_cash": effective_cash,
                    "cash_alloc_pct": cash_alloc,
                    "target_slice_pct": target_slice,
                    "top_liq": top_liq.symbol if top_liq else "",
                    "top_acq": top_acq.symbol if top_acq else "",
                    "tickers": build_status_rows(rows, top_liq=top_liq, top_acq=top_acq),
                }
            )
        except Exception:
            pass

    reconstructed = stock_total + options_total + effective_cash
    print("[RECONCILIATION]")
    print(f"  Stock total + options total + effective cash = {_fmt_money(reconstructed)}")
    print(f"  Equity (reported)                           = {_fmt_money(equity)}")
    print(f"  Difference                                  = {_fmt_money(equity - reconstructed)}")
    print()

    return {
        "equity": equity,
        "stock_total": stock_total,
        "options_total": options_total,
        "effective_cash": effective_cash,
        "n_tickers": float(len(rows)),
    }


# -----------------------------
# Watch tier + triggers
# -----------------------------
def watch_snapshot(session_state: str) -> Dict:
    equity = get_portfolio_equity()
    positions = get_open_stock_positions()
    symbols, qty_by, _avg_by = get_symbols_and_qty_from_positions(positions)

    prices: Dict[str, float] = {}
    for sym in symbols:
        q = safe_stock_quote(sym)
        price = _price_from_quote(q, prefer_extended=session_state == "extended")
        if price is None:
            price = _safe_float(q.get("last_trade_price"), default=0.0)
        prices[sym] = float(price)

    return {"equity": equity, "symbols": symbols, "qty": qty_by, "prices": prices, "ts": time.time()}


def should_trigger(
    prev: Optional[Dict],
    curr: Dict,
    price_move_trigger_pct: float,
    equity_move_trigger_usd: float,
) -> Tuple[bool, str]:
    if prev is None:
        return True, "initial"

    if prev["symbols"] != curr["symbols"]:
        return True, "symbols_changed"

    if abs(curr["equity"] - prev["equity"]) >= equity_move_trigger_usd:
        return True, f"equity_moved_${abs(curr['equity'] - prev['equity']):.2f}"

    for sym in curr["symbols"]:
        p0 = prev["prices"].get(sym)
        p1 = curr["prices"].get(sym)
        if p0 is None or p1 is None or p0 <= 0:
            continue
        move_pct = abs((p1 - p0) / p0) * 100.0
        if move_pct >= price_move_trigger_pct:
            return True, f"{sym}_moved_{move_pct:.2f}%"

    return False, "no_change"


# -----------------------------
# IO helpers
# -----------------------------
def load_params(params_path: str) -> Dict[str, Any]:
    p = Path(params_path)
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("params-json must be a JSON object.")
    return obj


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, help="Run directory allocated by the web app.")
    ap.add_argument("--params-json", required=True, help="Path to params.json provided by the web app.")
    ap.add_argument("--db-path", required=True, help="Path to Cryptid Exchange sqlite DB.")
    ap.add_argument("--connection-id", required=True, type=int, help="broker_connections.id for Robinhood.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    status_path = run_dir / "status.json"
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload["ts"] = iso_now()
        payload["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        payload["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(payload)
        except Exception:
            pass

    def bump_heartbeat(reason: str) -> None:
        payload = dict(last_status) if last_status else {"phase": "watching"}
        payload["reason"] = reason
        write_status(payload)

    params = load_params(args.params_json)

    global ENABLE_ANSI_COLORS
    ENABLE_ANSI_COLORS = bool(params.get("enable_ansi_colors", True))

    timeframe_key = str(params.get("timeframe", "1h")).strip()
    if timeframe_key not in TIMEFRAMES:
        print(f"[WARN] Invalid timeframe '{timeframe_key}'. Defaulting to '1h'.")
        timeframe_key = "1h"

    shares_per_loop = int(params.get("shares_per_trade_loop", DEFAULT_SHARES_PER_TRADE_LOOP))
    if shares_per_loop < 1:
        print(f"[WARN] shares_per_trade_loop must be >= 1. Defaulting to {DEFAULT_SHARES_PER_TRADE_LOOP}.")
        shares_per_loop = DEFAULT_SHARES_PER_TRADE_LOOP

    trading_enabled = bool(params.get("trading_enabled", False))
    top_n = int(params.get("top_n", 5))

    price_move_trigger_pct = float(params.get("price_move_trigger_pct", 0.25))
    equity_move_trigger_usd = float(params.get("equity_move_trigger_usd", 5.0))
    max_silent_seconds = int(params.get("max_silent_seconds", 300))
    watch_interval_seconds = int(params.get("watch_interval_seconds", 10))

    # Ensure broker session is live before loop
    login(args.db_path, int(args.connection_id), "", "", "")

    print(f"Using timeframe: {timeframe_key} ({TIMEFRAMES[timeframe_key]['interval']}, {TIMEFRAMES[timeframe_key]['span']})")
    print(f"Shares per trade loop: {shares_per_loop} (whole shares)")
    print(f"Trading enabled: {'YES' if trading_enabled else 'NO'}")

    prev_snap: Optional[Dict] = None
    last_full_run = 0.0

    session_state = ""
    while True:
        try:
            next_state = _market_session_state()
            if next_state != session_state:
                session_state = next_state
                if session_state == "regular":
                    print("Session: regular market hours (trailing stop sells enabled).")
                elif session_state == "extended":
                    print("Session: extended hours (limit orders at bid/ask midpoint).")
                else:
                    print("Session: market closed (orders skipped).")

            curr_snap = watch_snapshot(session_state)
            trigger, reason = should_trigger(
                prev_snap,
                curr_snap,
                price_move_trigger_pct=price_move_trigger_pct,
                equity_move_trigger_usd=equity_move_trigger_usd,
            )

            if (time.time() - last_full_run) >= float(max_silent_seconds):
                trigger = True
                reason = f"max_silent_{max_silent_seconds}s"

            if trigger:
                print(f"\n[TRIGGER] Running FoxBalance cycle (reason: {reason})\n")
                fox_balance_cycle(
                    timeframe_key=timeframe_key,
                    top_n=top_n,
                    trading_enabled=trading_enabled,
                    shares_per_loop=shares_per_loop,
                    session_state=session_state,
                    trade_stats=trade_stats,
                    status_writer=write_status,
                    status_reason=reason,
                )
                last_full_run = time.time()
            else:
                bump_heartbeat(reason)

            prev_snap = curr_snap
            time.sleep(max(0.0, float(watch_interval_seconds)))

        except KeyboardInterrupt:
            print("\nExiting FoxBalance loop.")
            return 0
        except Exception as e:
            print(f"[WARN] Loop error: {e} (retrying in 15s)")
            print(traceback.format_exc())
            time.sleep(15)


if __name__ == "__main__":
    raise SystemExit(main())
